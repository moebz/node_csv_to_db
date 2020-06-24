const fs = require("fs");
const csv = require("fast-csv");

const Pool = require("pg").Pool;
const pool = new Pool({
  user: "user",
  host: "host",
  database: "database",
  password: "password",
  port: 5432,
});

const csv_file = "paraguay_2012_barrrios_y_localidades.csv";

start();

function start() {
  try {
    createTables();
  } catch (e) {
    console.log("Coudln't create tables: ", e);
    return;
  }
  try {
    importCsvToPosgres(csv_file);
  } catch (e) {
    console.log("Coudln't finish importing: ", e);
  }
}

async function createTables() {
  await queryPromisified(
    `CREATE TABLE IF NOT EXISTS departamento (
      id int4 NOT NULL,
      descripcion varchar(255) NULL,
      CONSTRAINT departamento_pk PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS ciudad (
      id serial NOT NULL,
      descripcion varchar(255) NULL,
      departamento_id int4 NULL,
      CONSTRAINT ciudad_pk PRIMARY KEY (id)
    );
    ALTER TABLE ciudad ADD CONSTRAINT ciudad_fk FOREIGN KEY (departamento_id) REFERENCES departamento(id);
    CREATE TABLE IF NOT EXISTS barrio (
      id int4 NOT NULL,
      descripcion varchar(255) NULL,
      ciudad_id int4 NULL,
      CONSTRAINT barrio_pk PRIMARY KEY (id)
    );
    ALTER TABLE barrio ADD CONSTRAINT barrio_fk FOREIGN KEY (ciudad_id) REFERENCES ciudad(id);`,
    []
  );
}

async function importCsvToPosgres(filename) {
  let stream = fs.createReadStream(filename);
  let csvData = [];
  let csvStream = csv
    .parse()
    .on("data", function (data) {
      csvData.push(data);
    })
    .on("end", async function () {
      // Remove header row
      csvData.shift();

      try {
        for (const value of csvData) {
          let insert_departamento_result = await queryPromisified(
            "INSERT INTO departamento (id, descripcion) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT departamento_pk DO NOTHING RETURNING *",
            [value[1], value[3]]
          );
          let last_inserted_departamento_id =
            insert_departamento_result.rows &&
            insert_departamento_result.rows[0]
              ? insert_departamento_result.rows[0].id
              : null;
          console.log(
            "insert_departamento_result.last_inserted_departamento_id",
            last_inserted_departamento_id
          );

          let select_ciudad_result = await queryPromisified(
            "SELECT * FROM ciudad WHERE descripcion = $1",
            [value[4].trim().toUpperCase()]
          );
          console.log(
            "select_ciudad_result.rowCount",
            select_ciudad_result.rowCount
          );

          if (select_ciudad_result.rowCount > 0) {
            await insertBarrio(
              value[9],
              value[7],
              select_ciudad_result.rows[0].id
            );
          } else {
            let insert_ciudad_result = await queryPromisified(
              "INSERT INTO ciudad (descripcion, departamento_id) VALUES ($1, $2) RETURNING *",
              [value[4].trim().toUpperCase(), value[1]]
            );
            let last_inserted_ciudad_id =
              insert_ciudad_result.rows && insert_ciudad_result.rows[0]
                ? insert_ciudad_result.rows[0].id
                : null;
            console.log(
              "insert_ciudad_result.last_inserted_ciudad_id",
              last_inserted_ciudad_id
            );
            await insertBarrio(value[9], value[7], last_inserted_ciudad_id);
          }
          console.log("end loop");
        } // end loop
        console.log("end");
      } catch (error) {
        console.log("Error while inserting data", error);
      }
    });

  stream.pipe(csvStream);
}

function queryPromisified(queryString, values) {
  return new Promise((resolve, reject) => {
    pool.query(queryString, values, (error, results) => {
      if (error) {
        reject(error);
      }
      resolve(results);
    });
  });
}

async function insertBarrio(id, descripcion, ciudad_id) {
  await queryPromisified(
    "INSERT INTO barrio (id, descripcion, ciudad_id) VALUES ($1, $2, $3)",
    [id, descripcion, ciudad_id]
  );
}