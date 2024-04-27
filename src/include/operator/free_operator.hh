#pragma once

#include "operator/physical_operator.hh"

#include <functional>
#include <string>

class FreeOperator : public PhysicalOperator {
  std::function<void(FreeOperator *)> f;
  std::string desc;
  inline virtual void run_internal() override { f(this); }
  inline virtual std::string short_description() override { return desc; }

public:
  inline FreeOperator(std::string to_do_what,
                      std::function<void(FreeOperator *)> f)
      : f(f), desc(to_do_what){};
};
