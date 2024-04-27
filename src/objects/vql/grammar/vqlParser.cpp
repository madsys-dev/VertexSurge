
// Generated from
// /home/xwy/Project/recursive-join/src/objects/vql/grammar/vql.g4 by
// ANTLR 4.13.1

#include "vqlListener.h"

#include "vqlParser.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct VqlParserStaticData final {
  VqlParserStaticData(std::vector<std::string> ruleNames,
                      std::vector<std::string> literalNames,
                      std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  VqlParserStaticData(const VqlParserStaticData &) = delete;
  VqlParserStaticData(VqlParserStaticData &&) = delete;
  VqlParserStaticData &operator=(const VqlParserStaticData &) = delete;
  VqlParserStaticData &operator=(VqlParserStaticData &&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag vqlParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    VqlParserStaticData *vqlParserStaticData = nullptr;

void vqlParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (vqlParserStaticData != nullptr) {
    return;
  }
#else
  assert(vqlParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<VqlParserStaticData>(
      std::vector<std::string>{"var_prop", "var_props", "int_range",
                               "property_filter", "vertex_filter", "vertex",
                               "fixed_length", "variable_length", "length",
                               "shortest", "forward_edge", "backward_edge",
                               "undirected_edge", "edge", "path_append",
                               "pattern", "patterns", "count_only", "query"},
      std::vector<std::string>{
          "",     "'.'",  "','",        "'-'",      "'id:'",    "'tag:'", "'{'",
          "'}'",  "'('",  "')'",        "'['",      "']'",      "'..'",   "'s'",
          "'->'", "'<-'", "'count of'", "'vmatch'", "'return'", "';'"},
      std::vector<std::string>{"", "", "", "", "",    "",   "",   "",
                               "", "", "", "", "",    "",   "",   "",
                               "", "", "", "", "INT", "WS", "VAR"});
  static const int32_t serializedATNSegment[] = {
      4,   1,   22,  157, 2,   0,   7,   0,   2,   1,   7,   1,  2,   2,   7,
      2,   2,   3,   7,   3,   2,   4,   7,   4,   2,   5,   7,  5,   2,   6,
      7,   6,   2,   7,   7,   7,   2,   8,   7,   8,   2,   9,  7,   9,   2,
      10,  7,   10,  2,   11,  7,   11,  2,   12,  7,   12,  2,  13,  7,   13,
      2,   14,  7,   14,  2,   15,  7,   15,  2,   16,  7,   16, 2,   17,  7,
      17,  2,   18,  7,   18,  1,   0,   1,   0,   1,   0,   3,  0,   42,  8,
      0,   1,   1,   1,   1,   1,   1,   5,   1,   47,  8,   1,  10,  1,   12,
      1,   50,  9,   1,   1,   2,   1,   2,   1,   2,   1,   2,  1,   3,   1,
      3,   1,   3,   1,   3,   3,   3,   60,  8,   3,   1,   4,  1,   4,   1,
      4,   1,   4,   1,   5,   1,   5,   1,   5,   3,   5,   69, 8,   5,   1,
      5,   1,   5,   1,   6,   1,   6,   3,   6,   75,  8,   6,  1,   6,   1,
      6,   1,   7,   1,   7,   3,   7,   81,  8,   7,   1,   7,  1,   7,   1,
      7,   1,   7,   1,   8,   1,   8,   3,   8,   89,  8,   8,  1,   9,   1,
      9,   1,   10,  1,   10,  1,   10,  3,   10,  96,  8,   10, 1,   10,  1,
      10,  1,   10,  3,   10,  101, 8,   10,  1,   11,  1,   11, 1,   11,  3,
      11,  106, 8,   11,  1,   11,  1,   11,  1,   11,  3,   11, 111, 8,   11,
      1,   12,  1,   12,  1,   12,  3,   12,  116, 8,   12,  1,  12,  1,   12,
      1,   12,  3,   12,  121, 8,   12,  1,   13,  1,   13,  1,  13,  3,   13,
      126, 8,   13,  1,   14,  1,   14,  1,   14,  1,   15,  1,  15,  5,   15,
      133, 8,   15,  10,  15,  12,  15,  136, 9,   15,  1,   16, 1,   16,  1,
      16,  5,   16,  141, 8,   16,  10,  16,  12,  16,  144, 9,  16,  1,   17,
      1,   17,  1,   18,  1,   18,  1,   18,  1,   18,  3,   18, 152, 8,   18,
      1,   18,  1,   18,  1,   18,  1,   18,  0,   0,   19,  0,  2,   4,   6,
      8,   10,  12,  14,  16,  18,  20,  22,  24,  26,  28,  30, 32,  34,  36,
      0,   0,   155, 0,   38,  1,   0,   0,   0,   2,   43,  1,  0,   0,   0,
      4,   51,  1,   0,   0,   0,   6,   59,  1,   0,   0,   0,  8,   61,  1,
      0,   0,   0,   10,  65,  1,   0,   0,   0,   12,  72,  1,  0,   0,   0,
      14,  78,  1,   0,   0,   0,   16,  88,  1,   0,   0,   0,  18,  90,  1,
      0,   0,   0,   20,  100, 1,   0,   0,   0,   22,  110, 1,  0,   0,   0,
      24,  120, 1,   0,   0,   0,   26,  125, 1,   0,   0,   0,  28,  127, 1,
      0,   0,   0,   30,  130, 1,   0,   0,   0,   32,  137, 1,  0,   0,   0,
      34,  145, 1,   0,   0,   0,   36,  147, 1,   0,   0,   0,  38,  41,  5,
      22,  0,   0,   39,  40,  5,   1,   0,   0,   40,  42,  5,  22,  0,   0,
      41,  39,  1,   0,   0,   0,   41,  42,  1,   0,   0,   0,  42,  1,   1,
      0,   0,   0,   43,  48,  3,   0,   0,   0,   44,  45,  5,  2,   0,   0,
      45,  47,  3,   0,   0,   0,   46,  44,  1,   0,   0,   0,  47,  50,  1,
      0,   0,   0,   48,  46,  1,   0,   0,   0,   48,  49,  1,  0,   0,   0,
      49,  3,   1,   0,   0,   0,   50,  48,  1,   0,   0,   0,  51,  52,  5,
      20,  0,   0,   52,  53,  5,   3,   0,   0,   53,  54,  5,  20,  0,   0,
      54,  5,   1,   0,   0,   0,   55,  56,  5,   4,   0,   0,  56,  60,  5,
      20,  0,   0,   57,  58,  5,   5,   0,   0,   58,  60,  3,  4,   2,   0,
      59,  55,  1,   0,   0,   0,   59,  57,  1,   0,   0,   0,  60,  7,   1,
      0,   0,   0,   61,  62,  5,   6,   0,   0,   62,  63,  3,  6,   3,   0,
      63,  64,  5,   7,   0,   0,   64,  9,   1,   0,   0,   0,  65,  66,  5,
      8,   0,   0,   66,  68,  5,   22,  0,   0,   67,  69,  3,  8,   4,   0,
      68,  67,  1,   0,   0,   0,   68,  69,  1,   0,   0,   0,  69,  70,  1,
      0,   0,   0,   70,  71,  5,   9,   0,   0,   71,  11,  1,  0,   0,   0,
      72,  74,  5,   10,  0,   0,   73,  75,  5,   20,  0,   0,  74,  73,  1,
      0,   0,   0,   74,  75,  1,   0,   0,   0,   75,  76,  1,  0,   0,   0,
      76,  77,  5,   11,  0,   0,   77,  13,  1,   0,   0,   0,  78,  80,  5,
      10,  0,   0,   79,  81,  5,   20,  0,   0,   80,  79,  1,  0,   0,   0,
      80,  81,  1,   0,   0,   0,   81,  82,  1,   0,   0,   0,  82,  83,  5,
      12,  0,   0,   83,  84,  5,   20,  0,   0,   84,  85,  5,  11,  0,   0,
      85,  15,  1,   0,   0,   0,   86,  89,  3,   12,  6,   0,  87,  89,  3,
      14,  7,   0,   88,  86,  1,   0,   0,   0,   88,  87,  1,  0,   0,   0,
      89,  17,  1,   0,   0,   0,   90,  91,  5,   13,  0,   0,  91,  19,  1,
      0,   0,   0,   92,  93,  5,   3,   0,   0,   93,  95,  3,  16,  8,   0,
      94,  96,  3,   18,  9,   0,   95,  94,  1,   0,   0,   0,  95,  96,  1,
      0,   0,   0,   96,  97,  1,   0,   0,   0,   97,  98,  5,  14,  0,   0,
      98,  101, 1,   0,   0,   0,   99,  101, 5,   14,  0,   0,  100, 92,  1,
      0,   0,   0,   100, 99,  1,   0,   0,   0,   101, 21,  1,  0,   0,   0,
      102, 103, 5,   15,  0,   0,   103, 105, 3,   16,  8,   0,  104, 106, 3,
      18,  9,   0,   105, 104, 1,   0,   0,   0,   105, 106, 1,  0,   0,   0,
      106, 107, 1,   0,   0,   0,   107, 108, 5,   3,   0,   0,  108, 111, 1,
      0,   0,   0,   109, 111, 5,   15,  0,   0,   110, 102, 1,  0,   0,   0,
      110, 109, 1,   0,   0,   0,   111, 23,  1,   0,   0,   0,  112, 113, 5,
      3,   0,   0,   113, 115, 3,   16,  8,   0,   114, 116, 3,  18,  9,   0,
      115, 114, 1,   0,   0,   0,   115, 116, 1,   0,   0,   0,  116, 117, 1,
      0,   0,   0,   117, 118, 5,   3,   0,   0,   118, 121, 1,  0,   0,   0,
      119, 121, 5,   3,   0,   0,   120, 112, 1,   0,   0,   0,  120, 119, 1,
      0,   0,   0,   121, 25,  1,   0,   0,   0,   122, 126, 3,  20,  10,  0,
      123, 126, 3,   22,  11,  0,   124, 126, 3,   24,  12,  0,  125, 122, 1,
      0,   0,   0,   125, 123, 1,   0,   0,   0,   125, 124, 1,  0,   0,   0,
      126, 27,  1,   0,   0,   0,   127, 128, 3,   26,  13,  0,  128, 129, 3,
      10,  5,   0,   129, 29,  1,   0,   0,   0,   130, 134, 3,  10,  5,   0,
      131, 133, 3,   28,  14,  0,   132, 131, 1,   0,   0,   0,  133, 136, 1,
      0,   0,   0,   134, 132, 1,   0,   0,   0,   134, 135, 1,  0,   0,   0,
      135, 31,  1,   0,   0,   0,   136, 134, 1,   0,   0,   0,  137, 142, 3,
      30,  15,  0,   138, 139, 5,   2,   0,   0,   139, 141, 3,  30,  15,  0,
      140, 138, 1,   0,   0,   0,   141, 144, 1,   0,   0,   0,  142, 140, 1,
      0,   0,   0,   142, 143, 1,   0,   0,   0,   143, 33,  1,  0,   0,   0,
      144, 142, 1,   0,   0,   0,   145, 146, 5,   16,  0,   0,  146, 35,  1,
      0,   0,   0,   147, 148, 5,   17,  0,   0,   148, 149, 3,  32,  16,  0,
      149, 151, 5,   18,  0,   0,   150, 152, 3,   34,  17,  0,  151, 150, 1,
      0,   0,   0,   151, 152, 1,   0,   0,   0,   152, 153, 1,  0,   0,   0,
      153, 154, 3,   2,   1,   0,   154, 155, 5,   19,  0,   0,  155, 37,  1,
      0,   0,   0,   17,  41,  48,  59,  68,  74,  80,  88,  95, 100, 105, 110,
      115, 120, 125, 134, 142, 151};
  staticData->serializedATN = antlr4::atn::SerializedATNView(
      serializedATNSegment,
      sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i),
                                           i);
  }
  vqlParserStaticData = staticData.release();
}

} // namespace

vqlParser::vqlParser(TokenStream *input)
    : vqlParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

vqlParser::vqlParser(TokenStream *input,
                     const antlr4::atn::ParserATNSimulatorOptions &options)
    : Parser(input) {
  vqlParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *vqlParserStaticData->atn, vqlParserStaticData->decisionToDFA,
      vqlParserStaticData->sharedContextCache, options);
}

vqlParser::~vqlParser() { delete _interpreter; }

const atn::ATN &vqlParser::getATN() const { return *vqlParserStaticData->atn; }

std::string vqlParser::getGrammarFileName() const { return "vql.g4"; }

const std::vector<std::string> &vqlParser::getRuleNames() const {
  return vqlParserStaticData->ruleNames;
}

const dfa::Vocabulary &vqlParser::getVocabulary() const {
  return vqlParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView vqlParser::getSerializedATN() const {
  return vqlParserStaticData->serializedATN;
}

//----------------- Var_propContext
//------------------------------------------------------------------

vqlParser::Var_propContext::Var_propContext(ParserRuleContext *parent,
                                            size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> vqlParser::Var_propContext::VAR() {
  return getTokens(vqlParser::VAR);
}

tree::TerminalNode *vqlParser::Var_propContext::VAR(size_t i) {
  return getToken(vqlParser::VAR, i);
}

size_t vqlParser::Var_propContext::getRuleIndex() const {
  return vqlParser::RuleVar_prop;
}

void vqlParser::Var_propContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVar_prop(this);
}

void vqlParser::Var_propContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVar_prop(this);
}

vqlParser::Var_propContext *vqlParser::var_prop() {
  Var_propContext *_localctx =
      _tracker.createInstance<Var_propContext>(_ctx, getState());
  enterRule(_localctx, 0, vqlParser::RuleVar_prop);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(38);
    match(vqlParser::VAR);
    setState(41);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == vqlParser::T__0) {
      setState(39);
      match(vqlParser::T__0);
      setState(40);
      match(vqlParser::VAR);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Var_propsContext
//------------------------------------------------------------------

vqlParser::Var_propsContext::Var_propsContext(ParserRuleContext *parent,
                                              size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<vqlParser::Var_propContext *>
vqlParser::Var_propsContext::var_prop() {
  return getRuleContexts<vqlParser::Var_propContext>();
}

vqlParser::Var_propContext *vqlParser::Var_propsContext::var_prop(size_t i) {
  return getRuleContext<vqlParser::Var_propContext>(i);
}

size_t vqlParser::Var_propsContext::getRuleIndex() const {
  return vqlParser::RuleVar_props;
}

void vqlParser::Var_propsContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVar_props(this);
}

void vqlParser::Var_propsContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVar_props(this);
}

vqlParser::Var_propsContext *vqlParser::var_props() {
  Var_propsContext *_localctx =
      _tracker.createInstance<Var_propsContext>(_ctx, getState());
  enterRule(_localctx, 2, vqlParser::RuleVar_props);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(43);
    var_prop();
    setState(48);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == vqlParser::T__1) {
      setState(44);
      match(vqlParser::T__1);
      setState(45);
      var_prop();
      setState(50);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Int_rangeContext
//------------------------------------------------------------------

vqlParser::Int_rangeContext::Int_rangeContext(ParserRuleContext *parent,
                                              size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> vqlParser::Int_rangeContext::INT() {
  return getTokens(vqlParser::INT);
}

tree::TerminalNode *vqlParser::Int_rangeContext::INT(size_t i) {
  return getToken(vqlParser::INT, i);
}

size_t vqlParser::Int_rangeContext::getRuleIndex() const {
  return vqlParser::RuleInt_range;
}

void vqlParser::Int_rangeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInt_range(this);
}

void vqlParser::Int_rangeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInt_range(this);
}

vqlParser::Int_rangeContext *vqlParser::int_range() {
  Int_rangeContext *_localctx =
      _tracker.createInstance<Int_rangeContext>(_ctx, getState());
  enterRule(_localctx, 4, vqlParser::RuleInt_range);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(51);
    match(vqlParser::INT);
    setState(52);
    match(vqlParser::T__2);
    setState(53);
    match(vqlParser::INT);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Property_filterContext
//------------------------------------------------------------------

vqlParser::Property_filterContext::Property_filterContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *vqlParser::Property_filterContext::INT() {
  return getToken(vqlParser::INT, 0);
}

vqlParser::Int_rangeContext *vqlParser::Property_filterContext::int_range() {
  return getRuleContext<vqlParser::Int_rangeContext>(0);
}

size_t vqlParser::Property_filterContext::getRuleIndex() const {
  return vqlParser::RuleProperty_filter;
}

void vqlParser::Property_filterContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterProperty_filter(this);
}

void vqlParser::Property_filterContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitProperty_filter(this);
}

vqlParser::Property_filterContext *vqlParser::property_filter() {
  Property_filterContext *_localctx =
      _tracker.createInstance<Property_filterContext>(_ctx, getState());
  enterRule(_localctx, 6, vqlParser::RuleProperty_filter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(59);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
    case vqlParser::T__3: {
      enterOuterAlt(_localctx, 1);
      setState(55);
      match(vqlParser::T__3);
      setState(56);
      match(vqlParser::INT);
      break;
    }

    case vqlParser::T__4: {
      enterOuterAlt(_localctx, 2);
      setState(57);
      match(vqlParser::T__4);
      setState(58);
      int_range();
      break;
    }

    default:
      throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Vertex_filterContext
//------------------------------------------------------------------

vqlParser::Vertex_filterContext::Vertex_filterContext(ParserRuleContext *parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::Property_filterContext *
vqlParser::Vertex_filterContext::property_filter() {
  return getRuleContext<vqlParser::Property_filterContext>(0);
}

size_t vqlParser::Vertex_filterContext::getRuleIndex() const {
  return vqlParser::RuleVertex_filter;
}

void vqlParser::Vertex_filterContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVertex_filter(this);
}

void vqlParser::Vertex_filterContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVertex_filter(this);
}

vqlParser::Vertex_filterContext *vqlParser::vertex_filter() {
  Vertex_filterContext *_localctx =
      _tracker.createInstance<Vertex_filterContext>(_ctx, getState());
  enterRule(_localctx, 8, vqlParser::RuleVertex_filter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(61);
    match(vqlParser::T__5);
    setState(62);
    property_filter();
    setState(63);
    match(vqlParser::T__6);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VertexContext
//------------------------------------------------------------------

vqlParser::VertexContext::VertexContext(ParserRuleContext *parent,
                                        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *vqlParser::VertexContext::VAR() {
  return getToken(vqlParser::VAR, 0);
}

vqlParser::Vertex_filterContext *vqlParser::VertexContext::vertex_filter() {
  return getRuleContext<vqlParser::Vertex_filterContext>(0);
}

size_t vqlParser::VertexContext::getRuleIndex() const {
  return vqlParser::RuleVertex;
}

void vqlParser::VertexContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVertex(this);
}

void vqlParser::VertexContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVertex(this);
}

vqlParser::VertexContext *vqlParser::vertex() {
  VertexContext *_localctx =
      _tracker.createInstance<VertexContext>(_ctx, getState());
  enterRule(_localctx, 10, vqlParser::RuleVertex);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(65);
    match(vqlParser::T__7);
    setState(66);
    match(vqlParser::VAR);
    setState(68);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == vqlParser::T__5) {
      setState(67);
      vertex_filter();
    }
    setState(70);
    match(vqlParser::T__8);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Fixed_lengthContext
//------------------------------------------------------------------

vqlParser::Fixed_lengthContext::Fixed_lengthContext(ParserRuleContext *parent,
                                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode *vqlParser::Fixed_lengthContext::INT() {
  return getToken(vqlParser::INT, 0);
}

size_t vqlParser::Fixed_lengthContext::getRuleIndex() const {
  return vqlParser::RuleFixed_length;
}

void vqlParser::Fixed_lengthContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFixed_length(this);
}

void vqlParser::Fixed_lengthContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFixed_length(this);
}

vqlParser::Fixed_lengthContext *vqlParser::fixed_length() {
  Fixed_lengthContext *_localctx =
      _tracker.createInstance<Fixed_lengthContext>(_ctx, getState());
  enterRule(_localctx, 12, vqlParser::RuleFixed_length);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(72);
    match(vqlParser::T__9);
    setState(74);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == vqlParser::INT) {
      setState(73);
      match(vqlParser::INT);
    }
    setState(76);
    match(vqlParser::T__10);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Variable_lengthContext
//------------------------------------------------------------------

vqlParser::Variable_lengthContext::Variable_lengthContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode *> vqlParser::Variable_lengthContext::INT() {
  return getTokens(vqlParser::INT);
}

tree::TerminalNode *vqlParser::Variable_lengthContext::INT(size_t i) {
  return getToken(vqlParser::INT, i);
}

size_t vqlParser::Variable_lengthContext::getRuleIndex() const {
  return vqlParser::RuleVariable_length;
}

void vqlParser::Variable_lengthContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVariable_length(this);
}

void vqlParser::Variable_lengthContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVariable_length(this);
}

vqlParser::Variable_lengthContext *vqlParser::variable_length() {
  Variable_lengthContext *_localctx =
      _tracker.createInstance<Variable_lengthContext>(_ctx, getState());
  enterRule(_localctx, 14, vqlParser::RuleVariable_length);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(78);
    match(vqlParser::T__9);
    setState(80);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == vqlParser::INT) {
      setState(79);
      match(vqlParser::INT);
    }
    setState(82);
    match(vqlParser::T__11);
    setState(83);
    match(vqlParser::INT);
    setState(84);
    match(vqlParser::T__10);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LengthContext
//------------------------------------------------------------------

vqlParser::LengthContext::LengthContext(ParserRuleContext *parent,
                                        size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::Fixed_lengthContext *vqlParser::LengthContext::fixed_length() {
  return getRuleContext<vqlParser::Fixed_lengthContext>(0);
}

vqlParser::Variable_lengthContext *vqlParser::LengthContext::variable_length() {
  return getRuleContext<vqlParser::Variable_lengthContext>(0);
}

size_t vqlParser::LengthContext::getRuleIndex() const {
  return vqlParser::RuleLength;
}

void vqlParser::LengthContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLength(this);
}

void vqlParser::LengthContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLength(this);
}

vqlParser::LengthContext *vqlParser::length() {
  LengthContext *_localctx =
      _tracker.createInstance<LengthContext>(_ctx, getState());
  enterRule(_localctx, 16, vqlParser::RuleLength);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(88);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 6, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(86);
      fixed_length();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(87);
      variable_length();
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ShortestContext
//------------------------------------------------------------------

vqlParser::ShortestContext::ShortestContext(ParserRuleContext *parent,
                                            size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t vqlParser::ShortestContext::getRuleIndex() const {
  return vqlParser::RuleShortest;
}

void vqlParser::ShortestContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterShortest(this);
}

void vqlParser::ShortestContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitShortest(this);
}

vqlParser::ShortestContext *vqlParser::shortest() {
  ShortestContext *_localctx =
      _tracker.createInstance<ShortestContext>(_ctx, getState());
  enterRule(_localctx, 18, vqlParser::RuleShortest);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(90);
    match(vqlParser::T__12);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Forward_edgeContext
//------------------------------------------------------------------

vqlParser::Forward_edgeContext::Forward_edgeContext(ParserRuleContext *parent,
                                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::LengthContext *vqlParser::Forward_edgeContext::length() {
  return getRuleContext<vqlParser::LengthContext>(0);
}

vqlParser::ShortestContext *vqlParser::Forward_edgeContext::shortest() {
  return getRuleContext<vqlParser::ShortestContext>(0);
}

size_t vqlParser::Forward_edgeContext::getRuleIndex() const {
  return vqlParser::RuleForward_edge;
}

void vqlParser::Forward_edgeContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterForward_edge(this);
}

void vqlParser::Forward_edgeContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitForward_edge(this);
}

vqlParser::Forward_edgeContext *vqlParser::forward_edge() {
  Forward_edgeContext *_localctx =
      _tracker.createInstance<Forward_edgeContext>(_ctx, getState());
  enterRule(_localctx, 20, vqlParser::RuleForward_edge);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(100);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
    case vqlParser::T__2: {
      enterOuterAlt(_localctx, 1);
      setState(92);
      match(vqlParser::T__2);
      setState(93);
      length();
      setState(95);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == vqlParser::T__12) {
        setState(94);
        shortest();
      }
      setState(97);
      match(vqlParser::T__13);
      break;
    }

    case vqlParser::T__13: {
      enterOuterAlt(_localctx, 2);
      setState(99);
      match(vqlParser::T__13);
      break;
    }

    default:
      throw NoViableAltException(this);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Backward_edgeContext
//------------------------------------------------------------------

vqlParser::Backward_edgeContext::Backward_edgeContext(ParserRuleContext *parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::LengthContext *vqlParser::Backward_edgeContext::length() {
  return getRuleContext<vqlParser::LengthContext>(0);
}

vqlParser::ShortestContext *vqlParser::Backward_edgeContext::shortest() {
  return getRuleContext<vqlParser::ShortestContext>(0);
}

size_t vqlParser::Backward_edgeContext::getRuleIndex() const {
  return vqlParser::RuleBackward_edge;
}

void vqlParser::Backward_edgeContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBackward_edge(this);
}

void vqlParser::Backward_edgeContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBackward_edge(this);
}

vqlParser::Backward_edgeContext *vqlParser::backward_edge() {
  Backward_edgeContext *_localctx =
      _tracker.createInstance<Backward_edgeContext>(_ctx, getState());
  enterRule(_localctx, 22, vqlParser::RuleBackward_edge);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(110);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 10, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(102);
      match(vqlParser::T__14);
      setState(103);
      length();
      setState(105);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == vqlParser::T__12) {
        setState(104);
        shortest();
      }
      setState(107);
      match(vqlParser::T__2);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(109);
      match(vqlParser::T__14);
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Undirected_edgeContext
//------------------------------------------------------------------

vqlParser::Undirected_edgeContext::Undirected_edgeContext(
    ParserRuleContext *parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::LengthContext *vqlParser::Undirected_edgeContext::length() {
  return getRuleContext<vqlParser::LengthContext>(0);
}

vqlParser::ShortestContext *vqlParser::Undirected_edgeContext::shortest() {
  return getRuleContext<vqlParser::ShortestContext>(0);
}

size_t vqlParser::Undirected_edgeContext::getRuleIndex() const {
  return vqlParser::RuleUndirected_edge;
}

void vqlParser::Undirected_edgeContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUndirected_edge(this);
}

void vqlParser::Undirected_edgeContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUndirected_edge(this);
}

vqlParser::Undirected_edgeContext *vqlParser::undirected_edge() {
  Undirected_edgeContext *_localctx =
      _tracker.createInstance<Undirected_edgeContext>(_ctx, getState());
  enterRule(_localctx, 24, vqlParser::RuleUndirected_edge);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(120);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 12, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(112);
      match(vqlParser::T__2);
      setState(113);
      length();
      setState(115);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == vqlParser::T__12) {
        setState(114);
        shortest();
      }
      setState(117);
      match(vqlParser::T__2);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(119);
      match(vqlParser::T__2);
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- EdgeContext
//------------------------------------------------------------------

vqlParser::EdgeContext::EdgeContext(ParserRuleContext *parent,
                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::Forward_edgeContext *vqlParser::EdgeContext::forward_edge() {
  return getRuleContext<vqlParser::Forward_edgeContext>(0);
}

vqlParser::Backward_edgeContext *vqlParser::EdgeContext::backward_edge() {
  return getRuleContext<vqlParser::Backward_edgeContext>(0);
}

vqlParser::Undirected_edgeContext *vqlParser::EdgeContext::undirected_edge() {
  return getRuleContext<vqlParser::Undirected_edgeContext>(0);
}

size_t vqlParser::EdgeContext::getRuleIndex() const {
  return vqlParser::RuleEdge;
}

void vqlParser::EdgeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterEdge(this);
}

void vqlParser::EdgeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitEdge(this);
}

vqlParser::EdgeContext *vqlParser::edge() {
  EdgeContext *_localctx =
      _tracker.createInstance<EdgeContext>(_ctx, getState());
  enterRule(_localctx, 26, vqlParser::RuleEdge);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(125);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 13, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(122);
      forward_edge();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(123);
      backward_edge();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(124);
      undirected_edge();
      break;
    }

    default:
      break;
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Path_appendContext
//------------------------------------------------------------------

vqlParser::Path_appendContext::Path_appendContext(ParserRuleContext *parent,
                                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::EdgeContext *vqlParser::Path_appendContext::edge() {
  return getRuleContext<vqlParser::EdgeContext>(0);
}

vqlParser::VertexContext *vqlParser::Path_appendContext::vertex() {
  return getRuleContext<vqlParser::VertexContext>(0);
}

size_t vqlParser::Path_appendContext::getRuleIndex() const {
  return vqlParser::RulePath_append;
}

void vqlParser::Path_appendContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPath_append(this);
}

void vqlParser::Path_appendContext::exitRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPath_append(this);
}

vqlParser::Path_appendContext *vqlParser::path_append() {
  Path_appendContext *_localctx =
      _tracker.createInstance<Path_appendContext>(_ctx, getState());
  enterRule(_localctx, 28, vqlParser::RulePath_append);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(127);
    edge();
    setState(128);
    vertex();

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternContext
//------------------------------------------------------------------

vqlParser::PatternContext::PatternContext(ParserRuleContext *parent,
                                          size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::VertexContext *vqlParser::PatternContext::vertex() {
  return getRuleContext<vqlParser::VertexContext>(0);
}

std::vector<vqlParser::Path_appendContext *>
vqlParser::PatternContext::path_append() {
  return getRuleContexts<vqlParser::Path_appendContext>();
}

vqlParser::Path_appendContext *
vqlParser::PatternContext::path_append(size_t i) {
  return getRuleContext<vqlParser::Path_appendContext>(i);
}

size_t vqlParser::PatternContext::getRuleIndex() const {
  return vqlParser::RulePattern;
}

void vqlParser::PatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPattern(this);
}

void vqlParser::PatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPattern(this);
}

vqlParser::PatternContext *vqlParser::pattern() {
  PatternContext *_localctx =
      _tracker.createInstance<PatternContext>(_ctx, getState());
  enterRule(_localctx, 30, vqlParser::RulePattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(130);
    vertex();
    setState(134);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~0x3fULL) == 0) && ((1ULL << _la) & 49160) != 0)) {
      setState(131);
      path_append();
      setState(136);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PatternsContext
//------------------------------------------------------------------

vqlParser::PatternsContext::PatternsContext(ParserRuleContext *parent,
                                            size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<vqlParser::PatternContext *> vqlParser::PatternsContext::pattern() {
  return getRuleContexts<vqlParser::PatternContext>();
}

vqlParser::PatternContext *vqlParser::PatternsContext::pattern(size_t i) {
  return getRuleContext<vqlParser::PatternContext>(i);
}

size_t vqlParser::PatternsContext::getRuleIndex() const {
  return vqlParser::RulePatterns;
}

void vqlParser::PatternsContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPatterns(this);
}

void vqlParser::PatternsContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPatterns(this);
}

vqlParser::PatternsContext *vqlParser::patterns() {
  PatternsContext *_localctx =
      _tracker.createInstance<PatternsContext>(_ctx, getState());
  enterRule(_localctx, 32, vqlParser::RulePatterns);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(137);
    pattern();
    setState(142);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == vqlParser::T__1) {
      setState(138);
      match(vqlParser::T__1);
      setState(139);
      pattern();
      setState(144);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Count_onlyContext
//------------------------------------------------------------------

vqlParser::Count_onlyContext::Count_onlyContext(ParserRuleContext *parent,
                                                size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t vqlParser::Count_onlyContext::getRuleIndex() const {
  return vqlParser::RuleCount_only;
}

void vqlParser::Count_onlyContext::enterRule(
    tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCount_only(this);
}

void vqlParser::Count_onlyContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCount_only(this);
}

vqlParser::Count_onlyContext *vqlParser::count_only() {
  Count_onlyContext *_localctx =
      _tracker.createInstance<Count_onlyContext>(_ctx, getState());
  enterRule(_localctx, 34, vqlParser::RuleCount_only);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(145);
    match(vqlParser::T__15);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QueryContext
//------------------------------------------------------------------

vqlParser::QueryContext::QueryContext(ParserRuleContext *parent,
                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

vqlParser::PatternsContext *vqlParser::QueryContext::patterns() {
  return getRuleContext<vqlParser::PatternsContext>(0);
}

vqlParser::Var_propsContext *vqlParser::QueryContext::var_props() {
  return getRuleContext<vqlParser::Var_propsContext>(0);
}

vqlParser::Count_onlyContext *vqlParser::QueryContext::count_only() {
  return getRuleContext<vqlParser::Count_onlyContext>(0);
}

size_t vqlParser::QueryContext::getRuleIndex() const {
  return vqlParser::RuleQuery;
}

void vqlParser::QueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuery(this);
}

void vqlParser::QueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<vqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuery(this);
}

vqlParser::QueryContext *vqlParser::query() {
  QueryContext *_localctx =
      _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 36, vqlParser::RuleQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(147);
    match(vqlParser::T__16);
    setState(148);
    patterns();
    setState(149);
    match(vqlParser::T__17);
    setState(151);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == vqlParser::T__15) {
      setState(150);
      count_only();
    }
    setState(153);
    var_props();
    setState(154);
    match(vqlParser::T__18);

  } catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

void vqlParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  vqlParserInitialize();
#else
  ::antlr4::internal::call_once(vqlParserOnceFlag, vqlParserInitialize);
#endif
}
