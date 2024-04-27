#include "common/ptest_common.hh"
#include "operator/big_operators2.hh"
#include "plan/physical_plan.hh"
#include "plan/usual_pattern.hh"
#include "testify/fin_parameters.hh"

int main([[maybe_unused]] int argc, [[maybe_unused]] char *argv[]) {
  spdlog::set_level(spdlog::level::debug);

  Graph g(FIN_GRAPH_PATH_DEFAULT);
  g.load();

  VID id = 1;

  // auto person = inlined_vid("person.id", {id});
  // auto account = csr_expand_to_neighbor(person, "person.id", "account.id",
  //                                       "Person", "Account", "own");
  // auto other = vexpand(account, "account.id", "other.id", "Account",
  // "Account",
  //                      "transfer", VExpandPathParameters::Any(), 1, 3,
  //                      Direction::Backward);

  // auto other_flat = convert_to_flat(other);

  // auto loan = csr_expand_to_neighbor(other_flat, "other.id", "loan.id",
  //                                    "Account", "Loan", "deposit");

  // auto loan_with_balance =
  //     fetch_vertex_property(loan, "loan.id", "Loan", "balance");

  // auto other_and_balance = remove_variable(loan_with_balance, "loan.id");

  // auto other_and_sum_balance =
  //     sum_group_by(other_and_balance, "other.id", "balance");

  // // query_end(other_and_sum_balance);
  // query_end(loan_with_balance);

  PhysicalPlan p =
      PhysicalPlan()
          .inline_vid("person.id", {id})
          .csr_expand_to_neighbor("person.id", "account.id", "Person",
                                  "Account", "own")
          .vexpand("account.id", "other.id", "Account", "Account", "transfer",
                   VExpandPathParameters::Any(), 1, 3, Direction::Backward)
          .convert_to_flat()
          .csr_expand_to_neighbor("other.id", "loan.id", "Account", "Loan",
                                  "deposit")
          .fetch_vertex_property("loan.id", "Loan", "balance")
          .remove_variable("loan.id")
          .sum_group_by("other.id", "balance");
  // p.root->export_plan_png(fmt::format("resource/{}/plan.png", __FILE__));
  p.run();

  return 0;
}