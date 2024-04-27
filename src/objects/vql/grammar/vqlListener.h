
// Generated from
// /home/xwy/Project/recursive-join/src/objects/vql/grammar/vql.g4 by
// ANTLR 4.13.1

#pragma once

#include "antlr4-runtime.h"
#include "vqlParser.h"

/**
 * This interface defines an abstract listener for a parse tree produced by
 * vqlParser.
 */
class vqlListener : public antlr4::tree::ParseTreeListener {
public:
  virtual void enterVar_prop(vqlParser::Var_propContext *ctx) = 0;
  virtual void exitVar_prop(vqlParser::Var_propContext *ctx) = 0;

  virtual void enterVar_props(vqlParser::Var_propsContext *ctx) = 0;
  virtual void exitVar_props(vqlParser::Var_propsContext *ctx) = 0;

  virtual void enterInt_range(vqlParser::Int_rangeContext *ctx) = 0;
  virtual void exitInt_range(vqlParser::Int_rangeContext *ctx) = 0;

  virtual void enterProperty_filter(vqlParser::Property_filterContext *ctx) = 0;
  virtual void exitProperty_filter(vqlParser::Property_filterContext *ctx) = 0;

  virtual void enterVertex_filter(vqlParser::Vertex_filterContext *ctx) = 0;
  virtual void exitVertex_filter(vqlParser::Vertex_filterContext *ctx) = 0;

  virtual void enterVertex(vqlParser::VertexContext *ctx) = 0;
  virtual void exitVertex(vqlParser::VertexContext *ctx) = 0;

  virtual void enterFixed_length(vqlParser::Fixed_lengthContext *ctx) = 0;
  virtual void exitFixed_length(vqlParser::Fixed_lengthContext *ctx) = 0;

  virtual void enterVariable_length(vqlParser::Variable_lengthContext *ctx) = 0;
  virtual void exitVariable_length(vqlParser::Variable_lengthContext *ctx) = 0;

  virtual void enterLength(vqlParser::LengthContext *ctx) = 0;
  virtual void exitLength(vqlParser::LengthContext *ctx) = 0;

  virtual void enterShortest(vqlParser::ShortestContext *ctx) = 0;
  virtual void exitShortest(vqlParser::ShortestContext *ctx) = 0;

  virtual void enterForward_edge(vqlParser::Forward_edgeContext *ctx) = 0;
  virtual void exitForward_edge(vqlParser::Forward_edgeContext *ctx) = 0;

  virtual void enterBackward_edge(vqlParser::Backward_edgeContext *ctx) = 0;
  virtual void exitBackward_edge(vqlParser::Backward_edgeContext *ctx) = 0;

  virtual void enterUndirected_edge(vqlParser::Undirected_edgeContext *ctx) = 0;
  virtual void exitUndirected_edge(vqlParser::Undirected_edgeContext *ctx) = 0;

  virtual void enterEdge(vqlParser::EdgeContext *ctx) = 0;
  virtual void exitEdge(vqlParser::EdgeContext *ctx) = 0;

  virtual void enterPath_append(vqlParser::Path_appendContext *ctx) = 0;
  virtual void exitPath_append(vqlParser::Path_appendContext *ctx) = 0;

  virtual void enterPattern(vqlParser::PatternContext *ctx) = 0;
  virtual void exitPattern(vqlParser::PatternContext *ctx) = 0;

  virtual void enterPatterns(vqlParser::PatternsContext *ctx) = 0;
  virtual void exitPatterns(vqlParser::PatternsContext *ctx) = 0;

  virtual void enterCount_only(vqlParser::Count_onlyContext *ctx) = 0;
  virtual void exitCount_only(vqlParser::Count_onlyContext *ctx) = 0;

  virtual void enterQuery(vqlParser::QueryContext *ctx) = 0;
  virtual void exitQuery(vqlParser::QueryContext *ctx) = 0;
};
