#pragma once
#include <cstdint>
#include <filesystem>
#include <set>
#include <string>
#include <vector>

const size_t P_COMMUNITIES_DEFAULT[4][2] = {{0, 1}, {1, 2}, {2, 3}, {0, 1}};
// const size_t P_COMMUNITIES_DEFAULT[4][2] = {{0, 1}, {1, 2}, {3, 4}, {6, 7}};
// const size_t P_COMMUNITIES_DEFAULT[4][2] = {{3,4}, {1, 2}, {3, 4}, {6, 7}};

inline std::set<std::size_t> DEFAULT_COMMUNITY(size_t pi) {
  std::set<std::size_t> re;
  for (size_t i = P_COMMUNITIES_DEFAULT[pi][0];
       i < P_COMMUNITIES_DEFAULT[pi][1]; i++) {
    re.insert(i);
  }
  return re;
}

const size_t RISK_ID_DEFAULT[4][2] = {
    {0, 2000}, {2000, 4000}, {4000, 6000}, {300, 400}};

inline std::vector<std::size_t> DEFAULT_COMMUNITY_VEC(size_t pi) {
  std::vector<std::size_t> re;
  for (size_t i = RISK_ID_DEFAULT[pi][0]; i < RISK_ID_DEFAULT[pi][1]; i++) {
    re.push_back(i);
  }
  return re;
}

const std::size_t K_MIN_DEFAULT = 0, K_MAX_DEFAULT = 3, CT_K_MAX_DEFAULT = 2,
                  E6_K_MAX_DEFAULT = 6;
const bool ONLY_COUNT_DEFAULT = true;
// const bool ONLY_COUNT_DEFAULT = false;
// const std::string GRAPH_PATH_DEFAULT = "/home/xwy/ssd/rjq-dataset/sn-LastFM";
// const std::string GRAPH_PATH_DEFAULT =
// "/home/xwy/ssd/rjq-dataset/sn-Epinions1";
// const std::string GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset/sn-Example";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset48/sn-LastFM";

//     const std::string SN_GRAPH_PATH_DEFAULT =
// "/home/xwy/ssd/rjq-dataset48/sn-Epinions1";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset48/sn-ldbc_sf100";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/Dataset/rjq-dataset144/sn-LastFM";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/tmpfs/xeno4/rjq-dataset144/sn-LastFM";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-Epinions1";
// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf100";

const std::string SN_GRAPH_PATH_DEFAULT =
    "/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-ldbc_sf1000";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-LiveJournal1";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/tmpfs/xeno4/rjq-dataset1000/sn-twitter2010";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/Dataset/rjq-dataset1000/sn-twitter2010";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset48/sn-ldbc_sf1000";

// const std::string SN_GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset48/sn-LiveJournal1";

// const std::string BT_GRAPH_PATH_DEFAULT =
//     "/home/xwy/ssd/rjq-dataset48/bt-Rabobank";

// const std::string BT_GRAPH_PATH_DEFAULT =
// "/home/xwy/tmpfs/xeno4/rjq-dataset1000/bt-Rabobank";
const std::string BT_GRAPH_PATH_DEFAULT =
    "/home/xwy/tmpfs/xeno4/rjq-dataset1000/bt-ldbc_sf10";

const std::filesystem::path FIN_GRAPH_PATH_DEFAULT =
    "/home/xwy/tmpfs/xeno4/rjq-dataset1000/ldbc-fin-ldbc_sf10";

const bool PRINT_OUTPUT = true;
