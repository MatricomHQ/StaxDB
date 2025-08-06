{
  "targets": [
    {
      "target_name": "staxdb",
      "sources": [
        "stax_node_api/database_wrap.cpp",
        "stax_node_api/kv_transaction_wrap.cpp",
        "stax_node_api/graph_transaction_wrap.cpp",
        "../src/stax_api/staxdb_api.cpp",
        "../src/stax_common/os_file_extensions.cpp",
        "../src/stax_common/roaring.cpp",
        "../src/stax_core/stax_tree.cpp",
        "../src/stax_db/db.cpp",
        "../src/stax_db/query.cpp",
        "../src/stax_db/statistics.cpp",
        "../src/stax_graph/graph_engine.cpp"
      ],
      "include_dirs": [
        "../src",
        "<(module_root_dir)/node_modules/node-addon-api"
      ],
      "defines": [
        "NAPI_CPP_EXCEPTIONS",
        "NAPI_VERSION=9"
      ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "cflags": [
        "-std=c++20"
      ],
      "cflags_cc": [
        "-std=c++20"
      ],
      "xcode_settings": {
        "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
        "CLANG_CXX_LANGUAGE_STANDARD": "c++20",
        "MACOSX_DEPLOYMENT_TARGET": "11.0"
      },
      "msvs_settings": {
        "VCCLCompilerTool": {
          "ExceptionHandling": 1,
          "AdditionalOptions": ["/std:c++20"]
        }
      },
      "conditions": [
        ['OS=="win"', {
          "defines": ["NOMINMAX"],
        }],
        ['OS=="mac"', {
          'cflags+': [
            '-O3',
            '-Wno-unused-variable',
            '-Wno-deprecated-declarations'
          ],
          "link_settings": {
            "libraries": [ "-stdlib=libc++" ]
          }
        }],
        ['OS=="linux"', {
          'cflags+': [
            '-O3',
            '-Wno-unused-variable'
          ],
          "link_settings": {
            "libraries": ["-lrt", "-lpthread"]
          }
        }]
      ]
    }
  ]
}