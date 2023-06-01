const { Client } = require("@elastic/elasticsearch");
const fs = require("fs");
const express = require("express");
const cors = require("cors");

const client = new Client({
  cloud: {
    id: "344ac389bc7d477cb890044943205acd:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJGQ3MDBkNTk1MmJjMDRhMTdhYTg4OWE5NGMzN2JkNmE3JDI1Mjc5ZTk3ZTBiYzQwNTQ4NWMyZGEyYmYwYTRjNTU3",
  },
  auth: {
    username: "elastic",
    password: "ZnQQDhONioCdzBEbY2bN62NK",
  },
  tls: {
    ca: fs.readFileSync("./http_ca.crt"),
    rejectUnauthorized: false,
  },
});

const app = express();

// Middleware to parse JSON in requests
app.use(express.json());
app.use(cors());

client
  .info()
  .then((response) => console.log("Success", response))
  .catch((err) => console.log("fail", err));
// Endpoint to add a document to the Elasticsearch index
const products = JSON.parse(fs.readFileSync("csvecommercefinal.json"));

async function postProducts() {
  for (const product of products) {
    try {
      const response = await client.index({
        index: "productid",
        body: product,
      });
      console.log(response);
    } catch (error) {
      console.error(error);
    }
  }
}

// postProducts();
app.post("/product", async (req, res) => {
  try {
    const product = req.body;
    console.log("loook", req.body);
    const response = await client.index({
      index: "productid",
      body: product,
    });
    res.status(201).json(response.body);
    console.log(response);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to add document" });
  }
});

app.get("/product/:id", async (req, response, next) => {
  const { id } = req.params;
  try {
    const res = await client.search({
      index: "productid",
      body: {
        query: {
          match: { productid: id },
        },
      },
    });

    const resultat = res.hits.hits.map((hit) => hit._source);
    if (resultat.length > 0) {
      response.json(resultat);
    } else {
      response.send(null);
    }
  } catch (error) {
    console.error(error);
  }
});
// app.get("/product", async (req, response) => {
//   try {
//     const res = await client.search({
//       index: "productid",
//       // sort: { timestamp: "desc" },
//       size: 1000,
//       body: {
//         query: {
//           match_all: {},
//         },
//       },
//     });
//     const products = res.hits.hits.map((hit) => hit._source);

//     //  (res.hits.hits);
//     console.log(products);
//     response.send(products);
//   } catch (error) {
//     console.error(error);
//   }
// });
// app.get("/product", async (req, response) => {
//   try {
//     const scrollSize = 1000; // Set the scroll size per request
//     const params = {
//       index: "productid",
//       scroll: "1m", // Set the scroll timeout
//       body: {
//         size: scrollSize,
//         query: {
//           match_all: {},
//         },
//       },
//     };

//     const body = await client.search(params);
//     console.log(body);
//     response.send(body);
//   } catch (err) {
//     console.log(err);
//   }
// });
app.get("/product", async (req, response) => {
  try {
    const scrollSize = 500; // Set the scroll size per request
    const params = {
      index: "productid",
      scroll: "1m", // Set the scroll timeout
      body: {
        size: scrollSize,
        query: {
          match_all: {},
        },
      },
    };

    const body = await client.search(params);
    let products = body.hits.hits.map((hit) => hit._source);

    let scrollId = body._scroll_id;
    let totalHits = body.hits.total.value; // Total number of hits

    console.log(`Total Hits: ${totalHits}`);

    while (true) {
      const scrollParams = {
        scroll_id: scrollId,
        scroll: "1m",
      };

      const { body: nextBody } = await client.scroll(scrollParams);
      if (nextBody && nextBody.hits && nextBody.hits.hits.length > 0) {
        products = [
          ...products,
          ...nextBody.hits.hits.map((hit) => hit._source),
        ];
        scrollId = nextBody._scroll_id;
      } else {
        break; // Break out of the loop if there are no more hits
      }
    }

    response.send(products);
  } catch (error) {
    console.error(error);
    response.status(500).send("Internal Server Error");
  }
});

app.delete("/products", async (req, res) => {
  try {
    const { body: response } = await client.deleteByQuery({
      index: "productid",
      body: {
        query: {
          match_all: {},
        },
      },
    });
    res.json(response);
    console.log("done");
  } catch (error) {
    console.error(error);
    res.status(500).send("An error occurred while deleting products");
  }
});

// Adding orders

const orderIndex = "orderid";

app.post("/order", async (req, res) => {
  try {
    const order = req.body;
    const response = await client.index({
      index: orderIndex,
      body: order,
    });
    res.status(201).json(response.body);
    console.log(response);
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: "Failed to add order" });
  }
});

app.get("/order/:id", async (req, response, next) => {
  const { id } = req.params;
  try {
    const res = await client.search({
      index: orderIndex,
      body: {
        query: {
          match: { orderId: id },
        },
      },
    });

    const resultat = res.hits.hits.map((hit) => hit._source);
    if (resultat.length > 0) {
      response.json(resultat);
      // return resultat;
    } else {
      response.send(null);
    }
  } catch (error) {
    console.error(error);
  }
});

app.get("/order", async (req, response) => {
  try {
    const res = await client.search({
      index: orderIndex,
      body: {
        query: {
          match_all: {},
        },
      },
    });
    const orders = res.hits.hits.map((hit) => hit._source);

    console.log(orders);
    response.send(orders);
  } catch (error) {
    console.error(error);
  }
});

app.delete("/orders", async (req, res) => {
  try {
    const { body: response } = await client.deleteByQuery({
      index: orderIndex,
      body: {
        query: {
          match_all: {},
        },
      },
    });
    res.json(response);
    console.log("Orders deleted successfully");
  } catch (error) {
    console.error(error);
    res.status(500).send("An error occurred while deleting orders");
  }
});

app.listen(3005, () => {
  console.log("Server listening on port 3000");
});
