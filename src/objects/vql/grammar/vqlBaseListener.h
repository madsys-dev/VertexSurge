
// Generated from
// /home/xwy/Project/recursive-join/src/objects/vql/grammar/vql.g4 by
// ANTLR 4.13.1

#pragma once

#include "antlr4-runtime.h"
#include "vqlListener.h"

/**
 * This class provides an empty implementation of vqlListener,
 * which can be extended to create a listener which only needs to handle a
 * subset of the available methods.
 */
class vqlBaseListener : public vqlListener {
public:
  virtual void enterVar_prop(vqlParser::Var_propContext * /*ctx*/) override {}
  virtual void exitVar_prop(vqlParser::Var_propContext * /*ctx*/) override {}

  virtual void enterVar_props(vqlParser::Var_propsContext * /*ctx*/) override {}
  virtual void exitVar_props(vqlParser::Var_propsContext * /*ctx*/) override {}

  virtual void enterInt_range(vqlParser::Int_rangeContext * /*ctx*/) override {}
  virtual void exitInt_range(vqlParser::Int_rangeContext * /*ctx*/) override {}

  virtual void
  enterProperty_filter(vqlParser::Property_filterContext * /*ctx*/) override {}
  virtual void
  exitProperty_filter(vqlParser::Property_filterContext * /*ctx*/) override {}

  virtual void
  enterVertex_filter(vqlParser::Vertex_filterContext * /*ctx*/) override {}
  virtual void
  exitVertex_filter(vqlParser::Vertex_filterContext * /*ctx*/) override {}

  virtual void enterVertex(vqlParser::VertexContext * /*ctx*/) override {}
  virtual void exitVertex(vqlParser::VertexContext * /*ctx*/) override {}

  virtual void
  enterFixed_length(vqlParser::Fixed_lengthContext * /*ctx*/) override {}
  virtual void
  exitFixed_length(vqlParser::Fixed_lengthContext * /*ctx*/) override {}

  virtual void
  enterVariable_length(vqlParser::Variable_lengthContext * /*ctx*/) override {}
  virtual void
  exitVariable_length(vqlParser::Variable_lengthContext * /*ctx*/) override {}

  virtual void enterLength(vqlParser::LengthContext * /*ctx*/) override {}
  virtual void exitLength(vqlParser::LengthContext * /*ctx*/) override {}

  virtual void enterShortest(vqlParser::ShortestContext * /*ctx*/) override {}
  virtual void exitShortest(vqlParser::ShortestContext * /*ctx*/) override {}

  virtual void
  enterForward_edge(vqlParser::Forward_edgeContext * /*ctx*/) override {}
  virtual void
  exitForward_edge(vqlParser::Forward_edgeContext * /*ctx*/) override {}

  virtual void
  enterBackward_edge(vqlParser::Backward_edgeContext * /*ctx*/) override {}
  virtual void
  exitBackward_edge(vqlParser::Backward_edgeContext * /*ctx*/) override {}

  virtual void
  enterUndirected_edge(vqlParser::Undirected_edgeContext * /*ctx*/) override {}
  virtual void
  exitUndirected_edge(vqlParser::Undirected_edgeContext * /*ctx*/) override {}

  virtual void enterEdge(vqlParser::EdgeContext * /*ctx*/) override {}
  virtual void exitEdge(vqlParser::EdgeContext * /*ctx*/) override {}

  virtual void
  enterPath_append(vqlParser::Path_appendContext * /*ctx*/) override {}
  virtual void
  exitPath_append(vqlParser::Path_appendContext * /*ctx*/) override {}

  virtual void enterPattern(vqlParser::PatternContext * /*ctx*/) override {}
  virtual void exitPattern(vqlParser::PatternContext * /*ctx*/) override {}

  virtual void enterPatterns(vqlParser::PatternsContext * /*ctx*/) override {}
  virtual void exitPatterns(vqlParser::PatternsContext * /*ctx*/) override {}

  virtual void
  enterCount_only(vqlParser::Count_onlyContext * /*ctx*/) override {}
  virtual void exitCount_only(vqlParser::Count_onlyContext * /*ctx*/) override {
  }

  virtual void enterQuery(vqlParser::QueryContext * /*ctx*/) override {}
  virtual void exitQuery(vqlParser::QueryContext * /*ctx*/) override {}

  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override {}
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override {}
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override {}
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override {}
};
