---
title: How to use chainhooks in my application?
---

This guide helps you interact with the RESTful endpoints exposed in Chainhooks.

For instance, you can create a chainhook POST request by hitting http://oreo-alpha.testnet.hiro.so:20446/v1/chainhooks endpoint with the following JSON payload.

```JSON

{
    "stacks": {
      "network": "testnet",
      "version": 0,
      "start_block": 81094,
      "if_this": {
        "type": "print_event", // (1)
        "rule": {
          "contract_identifier": "ST1T9WVRB043VCBX61FSBR4006EHB75X9JDV2TG9G.liquidity-vault-v1-0",
          "contains": "add-asset-liquidity-vault-v1-0"
        }
      },
      "then_that": {
        "http": {
          "url": "YOUR_API_URL", // (2)
          "method": "POST",
          "authorization_header": "Bearer cn389ncoiwuencr"
        }
      }
    }
  },

```

Here, we are observing for print_event on the ST1T9WVRB043VCBX61FSBR4006EHB75X9JDV2TG9G.liquidity-vault-v1-0 that contains add-asset-liquidity-vault-v1-0 string literal in the log. Once hitting that combination, we trigger the HTTP POST.
