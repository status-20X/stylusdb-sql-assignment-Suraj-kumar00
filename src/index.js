const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

async function executeSELECTQuery(query) {
  // Check if the query is a non-empty string
  if (typeof query !== "string" || query.trim() === "") {
    throw new Error("Invalid query");
  }

  try {
    // Parse the query to extract fields and table name
    const { fields, table } = parseQuery(query);

    // Read the CSV file corresponding to the table name
    const data = await readCSV(`${table}.csv`);

    // Filter the fields based on the query
    return data.map((row) => {
      const filteredRow = {};
      // Check if each field exists in the row before accessing it
      fields.forEach((field) => {
        if (row.hasOwnProperty(field)) {
          filteredRow[field] = row[field];
        } else {
          // Handle the missing field (e.g., log a warning or skip it)
          console.warn(
            `Field '${field}' not found in CSV data for table '${table}'`
          );
        }
      });
      return filteredRow;
    });
  } catch (error) {
    // Handle any errors that occur during query execution
    console.error("Error executing SELECT query:", error.message);
    // Return an error response or rethrow the error depending on your use case
    throw error;
  }
}

module.exports = executeSELECTQuery;
