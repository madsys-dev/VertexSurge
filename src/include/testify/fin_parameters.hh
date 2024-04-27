#include "common.hh"
#include <nlohmann/json.hpp>

struct TCR1Params {
  VID id;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TCR1Params, id);
};

struct TCR2Params {
  VID id;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TCR2Params, id);
};

struct TCR3Params {
  VID id1;
  VID id2;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TCR3Params, id1, id2);
};

struct TCR6Params {
  VID id;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TCR6Params, id);
};

struct TCR8Params {
  VID id;
  NLOHMANN_DEFINE_TYPE_INTRUSIVE(TCR8Params, id);
};
