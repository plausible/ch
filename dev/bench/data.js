window.BENCHMARK_DATA = {
  "lastUpdate": 1776956357736,
  "repoUrl": "https://github.com/plausible/ch",
  "entries": {
    "Ch RowBinary Encode": [
      {
        "commit": {
          "author": {
            "email": "ruslandoga+gh@icloud.com",
            "name": "ruslandoga",
            "username": "ruslandoga"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f3739da0468c976ee11795ba444b400349d895d4",
          "message": "use benchmark-action/github-action-benchmark (#325)",
          "timestamp": "2026-04-23T17:53:17+03:00",
          "tree_id": "dea4f833af7cf7786d91f88081c05947d04a47b2",
          "url": "https://github.com/plausible/ch/commit/f3739da0468c976ee11795ba444b400349d895d4"
        },
        "date": 1776956131029,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Ch RowBinary Encode - RowBinary stream of 100k row chunks - 1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows",
            "value": 1.035,
            "range": "stddev 1.61%",
            "unit": "ips",
            "extra": "average: 966.16 ms\nmedian: 964.75 ms"
          },
          {
            "name": "Ch RowBinary Encode - RowBinary - 1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows",
            "value": 0.9932,
            "range": "stddev 14.9%",
            "unit": "ips",
            "extra": "average: 1006.83 ms\nmedian: 1077.91 ms"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ruslandoga+gh@icloud.com",
            "name": "ruslandoga",
            "username": "ruslandoga"
          },
          "committer": {
            "email": "ruslandoga+gh@icloud.com",
            "name": "ruslandoga",
            "username": "ruslandoga"
          },
          "distinct": true,
          "id": "0cd1cc6dbd63402b609d46605bb186af3c95dbb1",
          "message": "benchmark results are on github pages now",
          "timestamp": "2026-04-23T17:58:04+03:00",
          "tree_id": "f841595416049567569276cd24ba48190870b08c",
          "url": "https://github.com/plausible/ch/commit/0cd1cc6dbd63402b609d46605bb186af3c95dbb1"
        },
        "date": 1776956357232,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Ch RowBinary Encode - RowBinary stream of 100k row chunks - 1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows",
            "value": 1.0486,
            "range": "stddev 2.07%",
            "unit": "ips",
            "extra": "average: 953.63 ms\nmedian: 951.23 ms"
          },
          {
            "name": "Ch RowBinary Encode - RowBinary - 1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows",
            "value": 0.9878,
            "range": "stddev 15.83%",
            "unit": "ips",
            "extra": "average: 1012.36 ms\nmedian: 1066.39 ms"
          }
        ]
      }
    ]
  }
}