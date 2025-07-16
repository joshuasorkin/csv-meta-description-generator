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

// Define the response schema
const responseSchema = {
  type: "object",
  properties: {
    id: {
      type: "string",
      description: "The Shopify product ID"
    },
    product_title: {
      type: "string",
      description: "The original product title"
    },
    product_description: {
      type: "string", 
      description: "The original product description"
    },
    product_type: {
      type: "string",
      description: "The product category/type"
    },
    meta_description: {
      type: "string",
      description: "SEO-optimized meta description (150-160 characters)"
    }
  },
  required: ["id", "product_title", "product_description", "product_type", "meta_description"],
  additionalProperties: false
};

// Function to generate structured response using ChatGPT
async function generateStructuredResponse(id, productTitle, productDescription, productType) {
  try {
    const prompt = `Generate a compelling SEO meta description for this product. The meta description should be 150-160 characters, include the product name, highlight key benefits, and encourage clicks. Consider the product type/category when crafting the description.

Product Title: ${productTitle}
Product Description: ${productDescription}
Product Type: ${productType}

Return the response in the exact JSON format specified in the schema.`;

    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini', // Use gpt-4o-mini or gpt-4o for structured outputs
      messages: [
        {
          role: 'user',
          content: prompt
        }
      ],
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "product_meta_response",
          schema: responseSchema,
          strict: true
        }
      },
      temperature: 0.7
    });

    const result = JSON.parse(response.choices[0].message.content);
    return result;
  } catch (error) {
    console.error('Error generating structured response:', error.message);
    // Return fallback response in correct format
    return {
      id: id,
      product_title: productTitle,
      product_description: productDescription,
      product_type: productType,
      meta_description: 'Meta description generation failed'
    };
  }
}

// Function to process CSV in batches
async function processBatch(batch) {
  const promises = batch.map(async (row) => {
    const result = await generateStructuredResponse(
      row.id,
      row.title, 
      row.body_html,
      row.type
    );
    
    return result;
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
    //for (let i = 0; i < 3; i += BATCH_SIZE) {
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
        { id: 'id', title: 'ID' },
        { id: 'product_title', title: 'Title' },
        { id: 'product_description', title: 'Body HTML' },
        { id: 'product_type', title: 'Type' },
        { id: 'meta_description', title: 'Meta Description' }
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

module.exports = { processCSV, generateStructuredResponse };