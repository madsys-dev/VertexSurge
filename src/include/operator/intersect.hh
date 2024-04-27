#pragma once

#include "expand_impl.hh"
#include "physical_operator.hh"
#include "storage/CSR.hh"
#include <cstddef>
#include <tbb/concurrent_vector.h>



enum class IntersectStrategy {
  Adptive,
  CSR,
  BitMatrix,
};

template <typename M>
requires BitMatrix<M>
class IntersectMatrix : public PhysicalOperator {
  std::filesystem::path dir;

  M &left, &right;
  M &out;

  IntersectStrategy strategy;

  std::string desc;

  void do_matrix_intersect();

  virtual void run_internal() override;
  inline virtual std::string short_description() override { return desc; }

public:
  IntersectMatrix(std::string desc, M &left, M &right, M &out,
                  IntersectStrategy strategy);
};

////////////////////////////// TEMPLATE DEFINITION
