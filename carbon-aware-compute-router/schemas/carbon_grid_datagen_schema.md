{
  "properties": {
    "carbon_intensity_g_kwh": {
      "connect.index": 1,
      "connect.type": "float64",
      "type": "number"
    },
    "egress_cost_usd_gb": {
      "connect.index": 2,
      "connect.type": "float64",
      "type": "number"
    },
    "region": {
      "connect.index": 0,
      "type": "string"
    },
    "updated_at_epoch_ms": {
      "connect.index": 3,
      "connect.type": "int64",
      "type": "integer"
    }
  },
  "title": "com.carbon.router.CarbonGridRecord",
  "type": "object"
}