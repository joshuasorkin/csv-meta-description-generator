const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const OpenAI = require('openai');
require('dotenv').config();

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Configuration
const INPUT_CSV_PATH = 'products.csv';
const OUTPUT_CSV_PATH = 'products_with_meta.csv';
const BATCH_SIZE = 5; // Process 5 items at a time to avoid rate limits
const DELAY_BETWEEN_BATCHES = 1000; // 1 second delay between batches

// Function to generate meta description using ChatGPT
async function generateMetaDescription(productTitle, productDescription) {
  try {
    const prompt = `Generate a compelling SEO meta description for this product. The meta description should be 150-160 characters, include the product name, highlight key benefits, and encourage clicks.

Product Title: ${productTitle}
Product Description: ${productDescription}

Return only the meta description text, no additional formatting or explanation.`;

    const response = await openai.chat.completions.create({
      model: 'gpt-3.5-turbo',
      messages: [
        {
          role: 'user',
          content: prompt
        }
      ],
      max_tokens: 100,
      temperature: 0.7
    });

    return response.choices[0].message.content.trim();
  } catch (error) {
    console.error('Error generating meta description:', error.message);
    return 'Meta description generation failed';
  }
}

// Function to process CSV in batches
async function processBatch(batch) {
  const promises = batch.map(async (row) => {
    const metaDescription = await generateMetaDescription(
      row.product_title, 
      row.product_description
    );
    
    return {
      product_title: row.product_title,
      product_description: row.product_description,
      meta_description: metaDescription
    };
  });

  return Promise.all(promises);
}

// Function to delay execution
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Main function to process the CSV
async function processCSV() {
  try {
    console.log('Starting CSV processing...');
    
    // Read CSV file
    const products = [];
    
    await new Promise((resolve, reject) => {
      fs.createReadStream(INPUT_CSV_PATH)
        .pipe(csv())
        .on('data', (row) => {
          // Clean up column names (remove spaces, convert to lowercase)
          const cleanedRow = {};
          Object.keys(row).forEach(key => {
            const cleanKey = key.toLowerCase().replace(/\s+/g, '_');
            cleanedRow[cleanKey] = row[key];
          });
          products.push(cleanedRow);
        })
        .on('end', resolve)
        .on('error', reject);
    });

    console.log(`Found ${products.length} products to process`);

    // Process in batches
    const results = [];
    for (let i = 0; i < products.length; i += BATCH_SIZE) {
      const batch = products.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(products.length / BATCH_SIZE)}`);
      
      const batchResults = await processBatch(batch);
      results.push(...batchResults);
      
      // Add delay between batches to respect rate limits
      if (i + BATCH_SIZE < products.length) {
        await delay(DELAY_BETWEEN_BATCHES);
      }
    }

    // Write results to new CSV
    const csvWriter = createCsvWriter({
      path: OUTPUT_CSV_PATH,
      header: [
        { id: 'product_title', title: 'product_title' },
        { id: 'product_description', title: 'product_description' },
        { id: 'meta_description', title: 'meta_description' }
      ]
    });

    await csvWriter.writeRecords(results);
    console.log(`Processing complete! Results saved to ${OUTPUT_CSV_PATH}`);
    
  } catch (error) {
    console.error('Error processing CSV:', error);
  }
}

// Run the script
if (require.main === module) {
  processCSV();
}

module.exports = { processCSV, generateMetaDescription };