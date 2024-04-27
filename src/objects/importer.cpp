#include <filesystem>
#include <iostream>
#include <string>
#include <unistd.h>

#include "common.hh"
#include "meta/graph_loader.hh"
#include "meta/schema.hh"
using namespace std;

struct Args {
  filesystem::path input_dir;
  std::optional<filesystem::path> input_params_dir;
  filesystem::path output_dir;
  bool is_undirected = false;
  bool allow_multiple_edge = false;
  bool generate_penao_edges = false;
  std::optional<size_t> block_by_threads = std::nullopt;

  string graph_type, input_type, output_type;

  void load(int argc, char *argv[]) {
    int c;
    while ((c = getopt(argc, argv, "s:p:d:t:i:o:b:umh")) != -1) {
      switch (c) {
      case 's':
        input_dir = optarg;
        break;
      case 'p':
        input_params_dir = optarg;
        break;
      case 'd':
        output_dir = optarg;
        break;
      case 'u':
        is_undirected = true;
        break;
      case 'm':
        allow_multiple_edge = true;
        break;
      case 't': {
        graph_type = optarg;
        if (graph_type != "sn" && graph_type != "bt" &&
            graph_type != "ldbc-fin") {
          SPDLOG_ERROR("Unkown Graph Type");
          exit(0);
        }
        break;
      }
      case 'i':
        input_type = optarg;
        if (input_type != "ldbc-bi" && input_type != "ldbc-ic" &&
            input_type != "ldbc-fin" && input_type != "snap-txt" &&
            input_type != "snap-csv" && input_type != "rabo") {
          SPDLOG_ERROR("Unkown Input Type");
          exit(0);
        }
        break;
      case 'o': {
        output_type = optarg;
        if (output_type != "vlgpm" && output_type != "kuzu-csv" &&
            output_type != "tugraph" && output_type != "label") {
          SPDLOG_ERROR("Unkown Output Type");
          exit(0);
        }
        break;
      }
      case 'b': {
        block_by_threads = std::stoi(optarg);

        break;
      }
      case 'h': {
        generate_penao_edges = true;
        break;
      }
      default:
        std::cout << "Unknown option\n";
        break;
      }
    }
    if (input_dir == "" || output_dir == "" || input_type == "" ||
        output_type == "" || graph_type == "") {
      fmt::print("Usage: {}\n"
                 "-s : source file path, or source directory path\n"
                 "-p : params dir, for fin bench\n"
                 "-d : database dir\n"
                 "-u : treat edge as undirected(will add reverse edges)\n"
                 "-b : generate edge block by threads\n"
                 "-m : allow multiple edges\n"
                 "-t : imported graph type\n"
                 "     sn, Social Networks\n"
                 "     bt, Bank Transfer Graph\n"
                 "     ldbc-fin, LDBC FinBench\n"
                 "-i : input type\n"
                 "     ldbc-bi,  \n"
                 "     ldbc-ic,  \n"
                 "     ldbc-fin  \n"
                 "     snap-txt, \n"
                 "     snap-csv, \n"
                 "     rabo, \n"
                 "-o : output type\n"
                 "     vlgpm, \n"
                 "     kuzu-csv, \n"
                 "     tugraph, \n"
                 "     label, \n"
                 "-h : generate hilbert edges, \n",
                 argv[0]);
      exit(0);
    }
  }
};

int main(int argc, char *argv[]) {
  Args arg;
  arg.load(argc, argv);

  SPDLOG_INFO("Import {} from {} at {} to {} at {}", arg.graph_type,
              arg.input_type, arg.input_dir, arg.output_type, arg.output_dir);

  if (arg.graph_type == "sn") {
    SocialNetworkImporter sn;
    sn.import_config.input_dir = arg.input_dir;
    sn.import_config.output_dir = arg.output_dir;
    sn.import_config.input_dataset_name = arg.input_type;
    sn.import_config.output_graph_type = arg.graph_type;
    sn.import_config.target_database_name = arg.output_type;

    if (arg.input_type == "ldbc-bi") {
      sn.load_form_ldbc_bi(arg.input_dir);
    } else if (arg.input_type == "ldbc-ic") {
      sn.load_form_ldbc_ic(arg.input_dir);
    } else if (arg.input_type == "snap-txt") {
      sn.load_from_snap_txt(arg.input_dir);
    } else if (arg.input_type == "snap-csv") {
      sn.load_from_snap_csv(arg.input_dir);
    } else {
      assert(0);
    }

    if (arg.output_type == "vlgpm") {
      sn.output_vlgpm(arg.output_dir);
    } else if (arg.output_type == "kuzu-csv") {
      sn.output_kuzu_csv(arg.output_dir);
    } else if (arg.output_type == "tugraph") {
      SPDLOG_ERROR("Not implmented");
      assert(0);
    } else if (arg.output_type == "label") {
      sn.output_label_graph(arg.output_dir);
    } else {

      assert(0);
    }
  }

  else if (arg.graph_type == "bt") {
    TransferImporter bt;
    bt.import_config.input_dir = arg.input_dir;
    bt.import_config.output_dir = arg.output_dir;
    bt.import_config.input_dataset_name = arg.input_type;
    bt.import_config.output_graph_type = arg.graph_type;
    bt.import_config.target_database_name = arg.output_type;

    if (arg.input_type == "rabo") {
      bt.load_from_rabo(arg.input_dir);
    } else if (arg.input_type == "ldbc-fin") {
      bt.load_from_ldbc_fin(arg.input_dir);
    } else {
      assert(0);
    }

    if (arg.output_type == "vlgpm") {
      bt.output_vlgpm(arg.output_dir);
    } else if (arg.output_type == "kuzu-csv") {
      bt.output_kuzu_csv(arg.output_dir);
    } else if (arg.output_type == "tugraph") {
      assert(0);
    } else if (arg.output_type == "label") {
      bt.output_label_graph(arg.output_dir);
    } else {
      assert(0);
    }
  } else if (arg.graph_type == "ldbc-fin") {
    LDBCFinBenchImporter fin;
    fin.import_config.input_dir = arg.input_dir;
    fin.import_config.output_dir = arg.output_dir;
    fin.import_config.input_dataset_name = arg.input_type;
    fin.import_config.output_graph_type = arg.graph_type;
    fin.import_config.target_database_name = arg.output_type;
    if (arg.input_type == "ldbc-fin") {
      fin.load_from_ldbc_fin(arg.input_dir);
      fin.load_params_from_ldbc_fin(arg.input_params_dir.value());
    } else {
      assert(0);
    }
    fin.pcnt = arg.block_by_threads;

    if (arg.output_type == "vlgpm") {
      fin.output_vlgpm(arg.output_dir);
    } else if (arg.output_type == "kuzu-csv") {
      fin.output_kuzu_csv(arg.output_dir);
    } else if (arg.output_type == "tugraph") {
      assert(0);
    } else if (arg.output_type == "label") {
      fin.output_label_graph(arg.output_dir);
    } else {
      assert(0);
    }

  }

  else {
    SPDLOG_ERROR("No such graph type");
  }

  return 0;
}