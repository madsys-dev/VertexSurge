
// Generated from
// /home/xwy/Project/recursive-join/src/objects/vql/grammar/vql.g4 by
// ANTLR 4.13.1

#pragma once

#include "antlr4-runtime.h"

class vqlParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    T__4 = 5,
    T__5 = 6,
    T__6 = 7,
    T__7 = 8,
    T__8 = 9,
    T__9 = 10,
    T__10 = 11,
    T__11 = 12,
    T__12 = 13,
    T__13 = 14,
    T__14 = 15,
    T__15 = 16,
    T__16 = 17,
    T__17 = 18,
    T__18 = 19,
    INT = 20,
    WS = 21,
    VAR = 22
  };

  enum {
    RuleVar_prop = 0,
    RuleVar_props = 1,
    RuleInt_range = 2,
    RuleProperty_filter = 3,
    RuleVertex_filter = 4,
    RuleVertex = 5,
    RuleFixed_length = 6,
    RuleVariable_length = 7,
    RuleLength = 8,
    RuleShortest = 9,
    RuleForward_edge = 10,
    RuleBackward_edge = 11,
    RuleUndirected_edge = 12,
    RuleEdge = 13,
    RulePath_append = 14,
    RulePattern = 15,
    RulePatterns = 16,
    RuleCount_only = 17,
    RuleQuery = 18
  };

  explicit vqlParser(antlr4::TokenStream *input);

  vqlParser(antlr4::TokenStream *input,
            const antlr4::atn::ParserATNSimulatorOptions &options);

  ~vqlParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN &getATN() const override;

  const std::vector<std::string> &getRuleNames() const override;

  const antlr4::dfa::Vocabulary &getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class Var_propContext;
  class Var_propsContext;
  class Int_rangeContext;
  class Property_filterContext;
  class Vertex_filterContext;
  class VertexContext;
  class Fixed_lengthContext;
  class Variable_lengthContext;
  class LengthContext;
  class ShortestContext;
  class Forward_edgeContext;
  class Backward_edgeContext;
  class Undirected_edgeContext;
  class EdgeContext;
  class Path_appendContext;
  class PatternContext;
  class PatternsContext;
  class Count_onlyContext;
  class QueryContext;

  class Var_propContext : public antlr4::ParserRuleContext {
  public:
    Var_propContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> VAR();
    antlr4::tree::TerminalNode *VAR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Var_propContext *var_prop();

  class Var_propsContext : public antlr4::ParserRuleContext {
  public:
    Var_propsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Var_propContext *> var_prop();
    Var_propContext *var_prop(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Var_propsContext *var_props();

  class Int_rangeContext : public antlr4::ParserRuleContext {
  public:
    Int_rangeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> INT();
    antlr4::tree::TerminalNode *INT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Int_rangeContext *int_range();

  class Property_filterContext : public antlr4::ParserRuleContext {
  public:
    Property_filterContext(antlr4::ParserRuleContext *parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT();
    Int_rangeContext *int_range();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Property_filterContext *property_filter();

  class Vertex_filterContext : public antlr4::ParserRuleContext {
  public:
    Vertex_filterContext(antlr4::ParserRuleContext *parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Property_filterContext *property_filter();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Vertex_filterContext *vertex_filter();

  class VertexContext : public antlr4::ParserRuleContext {
  public:
    VertexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *VAR();
    Vertex_filterContext *vertex_filter();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  VertexContext *vertex();

  class Fixed_lengthContext : public antlr4::ParserRuleContext {
  public:
    Fixed_lengthContext(antlr4::ParserRuleContext *parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Fixed_lengthContext *fixed_length();

  class Variable_lengthContext : public antlr4::ParserRuleContext {
  public:
    Variable_lengthContext(antlr4::ParserRuleContext *parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> INT();
    antlr4::tree::TerminalNode *INT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Variable_lengthContext *variable_length();

  class LengthContext : public antlr4::ParserRuleContext {
  public:
    LengthContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Fixed_lengthContext *fixed_length();
    Variable_lengthContext *variable_length();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  LengthContext *length();

  class ShortestContext : public antlr4::ParserRuleContext {
  public:
    ShortestContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  ShortestContext *shortest();

  class Forward_edgeContext : public antlr4::ParserRuleContext {
  public:
    Forward_edgeContext(antlr4::ParserRuleContext *parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LengthContext *length();
    ShortestContext *shortest();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Forward_edgeContext *forward_edge();

  class Backward_edgeContext : public antlr4::ParserRuleContext {
  public:
    Backward_edgeContext(antlr4::ParserRuleContext *parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LengthContext *length();
    ShortestContext *shortest();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Backward_edgeContext *backward_edge();

  class Undirected_edgeContext : public antlr4::ParserRuleContext {
  public:
    Undirected_edgeContext(antlr4::ParserRuleContext *parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LengthContext *length();
    ShortestContext *shortest();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Undirected_edgeContext *undirected_edge();

  class EdgeContext : public antlr4::ParserRuleContext {
  public:
    EdgeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Forward_edgeContext *forward_edge();
    Backward_edgeContext *backward_edge();
    Undirected_edgeContext *undirected_edge();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  EdgeContext *edge();

  class Path_appendContext : public antlr4::ParserRuleContext {
  public:
    Path_appendContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    EdgeContext *edge();
    VertexContext *vertex();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Path_appendContext *path_append();

  class PatternContext : public antlr4::ParserRuleContext {
  public:
    PatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VertexContext *vertex();
    std::vector<Path_appendContext *> path_append();
    Path_appendContext *path_append(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  PatternContext *pattern();

  class PatternsContext : public antlr4::ParserRuleContext {
  public:
    PatternsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PatternContext *> pattern();
    PatternContext *pattern(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  PatternsContext *patterns();

  class Count_onlyContext : public antlr4::ParserRuleContext {
  public:
    Count_onlyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  Count_onlyContext *count_only();

  class QueryContext : public antlr4::ParserRuleContext {
  public:
    QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PatternsContext *patterns();
    Var_propsContext *var_props();
    Count_onlyContext *count_only();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;
  };

  QueryContext *query();

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

private:
};
