#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>

#include "common.hh"
#include "meta/type.hh"
#include "storage/bitmap.hh"
#include "storage/concurrent_disk_array.hh"
#include "storage/disk_array.hh"
#include "utils/multi_column_sort.hh"
#include "utils/peano.hh"

using namespace std;

TEST(DiskArray, TestSum) {
  return;
  DiskArray<int32_t> da("/tmp/vertex.bin", true);

  GTEST_ASSERT_EQ(da.size(), 0);
  int32_t c[1000];
  for (size_t i = 0; i < 1000; i++) {
    c[i] = i;
  }
  Timer t;
  t.start();
  // #pragma omp parallel for
  for (size_t i = 0; i < 10000000; i++) {
    da.push_many(c, 100);
  }
  GTEST_ASSERT_EQ(da.size(), 1000000000LL);
  // SPDLOG_INFO("Sum time: {}",t.elapsedTime());
  size_t sum = 0;
  for (size_t i = 0; i < da.size(); i++) {
    sum += da[i];
  }
  size_t expected = 99 * 100 / 2 * 10000000LL;

  // cout << expected << endl;
  // cout << sum << endl;
  // cout << "Delta: " << sum - expected << endl;
  GTEST_ASSERT_EQ(sum - expected, 0);
}

TEST(DiskArray, TestStore) {
  const size_t CN = 1000, PUSHN = 10000000, PUSHC = 100;
  int32_t c[CN];
  for (size_t i = 0; i < CN; i++) {
    c[i] = i;
  }
  {
    DiskArray<int32_t> da("/tmp/vertex.bin", true);
    GTEST_ASSERT_EQ(da.size(), 0);

    for (size_t i = 0; i < PUSHN; i++) {
      da.push_many(c, PUSHC);
    }
    GTEST_ASSERT_EQ(da.size(), PUSHN * PUSHC);
    size_t sum = 0;
    for (size_t i = 0; i < da.size(); i++) {
      sum += da[i];
    }
    size_t expected = (PUSHC - 1) * (PUSHC) / 2 * PUSHN;
    GTEST_ASSERT_EQ(sum, expected);
  }

  {
    DiskArray<int32_t> da("/tmp/vertex.bin", false);
    GTEST_ASSERT_EQ(da.size(), PUSHN * PUSHC);
    for (size_t i = 0; i < PUSHN; i++) {
      da.push_many(c, PUSHC);
    }
    GTEST_ASSERT_EQ(da.size(), 2 * PUSHC * PUSHN);
    size_t sum = 0;
    for (size_t i = 0; i < da.size(); i++) {
      sum += da[i];
    }
    size_t expected = (PUSHC - 1) * (PUSHC) / 2 * PUSHN * 2;
    GTEST_ASSERT_EQ(sum, expected);
  }
}

TEST(DiskArray, AnonymousInit) {
  const size_t C = 10000000;
  DiskArray<int64_t> da(C);
  for (size_t i = 0; i < C; i++) {
    da[i] = i;
  }
  for (size_t i = 0; i < C; i++) {
    EXPECT_EQ(da[i], i);
  }
}

TEST(DiskArray, NoType) {
  DiskArray<NoType> da;
  da.set_sizeof_data(sizeof(int32_t));
  const size_t X = 1000000;
  GTEST_ASSERT_EQ(da.size(), 0);

  for (size_t i = 0; i < X; i++) {
    int32_t x = i;
    da.push_back_nt(&x);
  }
  GTEST_ASSERT_EQ(da.size(), X);

  for (size_t i = 0; i < X; i++) {
    int32_t x = i, y;
    memcpy(&y, &da[i], sizeof(int32_t));
    EXPECT_EQ(x, y);
  }
}

TEST(DiskArray, NoType2) {
  DiskArray<NoType> da;
  da.set_sizeof_data(sizeof(ShortString));
  const size_t X = 1000000;
  GTEST_ASSERT_EQ(da.size(), 0);

  for (size_t i = 0; i < X; i++) {
    ShortString x = short_string_from_string(to_string(i));
    da.push_back_nt(&x);
  }
  GTEST_ASSERT_EQ(da.size(), X);

  for (size_t i = 0; i < X; i++) {
    ShortString x = short_string_from_string(to_string(i));
    ShortString y;
    memcpy(&y, &da[i], sizeof(ShortString));
    EXPECT_EQ(x, y);
  }
}

TEST(DiskArray, NoTypeNamed) {
  constexpr size_t CNT = 100000;
  {
    DiskArray<NoType> da("/tmp/tmp.bin", true);
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);

    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt(&x);
    }
    GTEST_ASSERT_EQ(da.size(), CNT);

    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i, y;
      memcpy(&y, &da[i], sizeof(int32_t));
      EXPECT_EQ(x, y);
    }
  }
  {
    DiskArray<NoType> da("/tmp/tmp.bin", false);
    GTEST_ASSERT_EQ(da.size(), CNT);
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i, y;
      memcpy(&y, &da[i], sizeof(int32_t));
      EXPECT_EQ(x, y);
      x += CNT;
      da.push_back_nt(&x);
    }
  }
  {
    DiskArray<NoType> da("/tmp/tmp.bin", false);
    GTEST_ASSERT_EQ(da.size(), 2 * CNT);
    for (size_t i = 0; i < 2 * CNT; i++) {
      int32_t x = i, y;
      memcpy(&y, &da[i], sizeof(int32_t));
      EXPECT_EQ(x, y);
    }
  }
}

TEST(DiskArray, ParallelPush) {
  constexpr size_t CNT = 100000;
  {
    DiskArray<NoType> da("/tmp/tmp.bin", true);
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt_atomic(&x);
    }
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &da[i], sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(DiskArray, Move) {
  constexpr size_t CNT = 100000;
  {
    std::unique_ptr<DiskArray<NoType>> da =
        make_unique<DiskArray<NoType>>("/tmp/tmp.bin", true);
    da->set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da->size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da->push_back_nt_atomic(&x);
    }
    GTEST_ASSERT_EQ(da->size(), CNT);

    std::unique_ptr<DiskArray<NoType>> another = std::move(da);
    EXPECT_EQ(another->size(), CNT);
    EXPECT_EQ(da, nullptr);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &((*another)[i]), sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    DiskArray<NoType> da("/tmp/tmp.bin", true);
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt_atomic(&x);
    }
    GTEST_ASSERT_EQ(da.size(), CNT);

    auto another = make_unique<DiskArray<NoType>>(std::move(da));
    EXPECT_EQ(another->size(), CNT);
    EXPECT_EQ(da.is_valid(), false);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &(*another)[i], sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, ConcurrentNoType) {
  constexpr size_t CNT = 100000;
  {
    ConcurrentDiskArray<NoType> da("/tmp/tmp", true);
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt(&x);
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &da[i], sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    ConcurrentDiskArray<NoType> da;
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt(&x);
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &da[i], sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, ConcurrentWithType) {
  constexpr size_t CNT = 100000;
  {
    ConcurrentDiskArray<int32_t> da("/tmp/tmp", true);
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = da[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    ConcurrentDiskArray<int32_t> da;
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = da[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, ParallelPushPerformance) {
  constexpr size_t CNT = 1e7;
  {
    Timer t;
    t.start();
    DiskArray<NoType> da;
    da.set_sizeof_data(sizeof(int32_t));
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back_nt_atomic(&x);
    }
    SPDLOG_INFO("DiskArray Throughput: {}", t.report_throughput(CNT));
    GTEST_ASSERT_EQ(da.size(), CNT);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      memcpy(&y, &da[i], sizeof(int32_t));
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    Timer t;
    t.start();
    ConcurrentDiskArray<int32_t> da;
    DiskArray<int32_t> daa;
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    GTEST_ASSERT_EQ(da.size(), CNT);
    da.update_index();
    da.merge_to_provided_array(daa);
    SPDLOG_INFO("ConcurrentDiskArray Throughput: {}", t.report_throughput(CNT));

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = daa[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, Move) {
  constexpr size_t CNT = 100000;
  {
    ConcurrentDiskArray<int32_t> da("/tmp/tmp", true);
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    auto another =
        std::make_unique<ConcurrentDiskArray<int32_t>>(std::move(da));

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = (*another)[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, MergeToFirst) {
  constexpr size_t CNT = 100000;
  {
    ConcurrentDiskArray<int32_t> da("/tmp/tmp", true);
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    auto another = da.merge_to_first_array();

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = (*another)[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    ConcurrentDiskArray<int32_t> da;
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    auto another = da.merge_to_first_array();

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = (*another)[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(ConcurrentDiskArray, MergeToProvided) {
  constexpr size_t CNT = 100000;
  {
    ConcurrentDiskArray<int32_t> da("/tmp/tmp", true);
    DiskArray<int32_t> daa("/tmp/tmp2", true);
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    da.merge_to_provided_array(daa);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = daa[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
  {
    ConcurrentDiskArray<int32_t> da;
    DiskArray<int32_t> daa;
    GTEST_ASSERT_EQ(da.size(), 0);
#pragma omp parallel for
    for (size_t i = 0; i < CNT; i++) {
      int32_t x = i;
      da.push_back(std::move(x));
    }
    da.update_index();
    GTEST_ASSERT_EQ(da.size(), CNT);

    da.merge_to_provided_array(daa);

    size_t sum = 0;
    for (size_t i = 0; i < CNT; i++) {
      int32_t y;
      y = daa[i];
      // cout<<i<<" "<<y<<endl;
      sum += y;
    }
    EXPECT_EQ(sum, CNT * (CNT - 1) / 2);
  }
}

TEST(Hilbert, Sort) {
  for (size_t i = 0; i < 16; i++) {
    for (size_t j = 0; j < 16; j++) {
      cout << XYToIndex(16, i, j, PeanoDirection::RA) << " ";
    }
    cout << endl;
  }
}

TEST(Hilbert, MultiSort) {
  const size_t N = 1024;
  const size_t N2 = round_to_2_power(N);
  std::mt19937 gen(123);
  vector<int32_t> x(N * N), y(N * N), h(N * N);
  using P = std::tuple<int32_t *, int32_t *, int32_t *>;

  for (size_t i = 0; i < N; i++) {
    for (size_t j = 0; j < N; j++) {
      x[i * N + j] = i;
      y[i * N + j] = j;
    }
  }
  // uniform_random_numbers(gen, N,x.data(), N);
  // uniform_random_numbers(gen, N, y.data(), N);

  Timer t;
  t.start();

  for (size_t i = 0; i < x.size(); i++) {
    h[i] = XYToIndex(N2, x[i], y[i], PeanoDirection::RA);
    // h[i] = dfs(x[i], y[i], 1,  10);
  }

  serialsort(
      P{x.data(), y.data(), h.data()}, 0, x.size(),
      [&N2](P &p, size_t i, size_t j) {
        return std::get<2>(p)[i] < std::get<2>(p)[j];
      },
      [](P &p, size_t i, size_t j) {
        swap(get<0>(p)[i], get<0>(p)[j]);
        swap(get<1>(p)[i], get<1>(p)[j]);
        swap(get<2>(p)[i], get<2>(p)[j]);
      });

  SPDLOG_INFO("{}", t.elapsedTime());

  for (size_t i = 1; i < x.size(); i++) {

    bool xs = x[i] == x[i - 1], ys = y[i] == y[i - 1];
    if (!(xs || ys)) {
      SPDLOG_INFO("{}: {} {}", i, x[i], y[i]);
    }
    assert(xs || ys);
  }
}

TEST(Hilbert, MultiSortRandom) {
  const size_t N = 1e6;
  const size_t N2 = round_to_2_power(N);
  const size_t AS = 1e7;
  std::mt19937 gen(123);
  vector<int32_t> x(AS), y(AS), h(AS);
  using P = std::tuple<int32_t *, int32_t *, int32_t *>;

  uniform_int_distribution<int32_t> dis(0, N);
  for (size_t i = 0; i < AS; i++) {
    x[i] = dis(gen);
    y[i] = dis(gen);
  }

  Timer t;
  t.start();

  for (size_t i = 0; i < x.size(); i++) {
    h[i] = XYToIndex(N2, x[i], y[i], PeanoDirection::RA);
    // h[i] = dfs(x[i], y[i], 1,  10);
  }

  serialsort(
      P{x.data(), y.data(), h.data()}, 0, x.size(),
      [&N2](P &p, size_t i, size_t j) {
        return std::get<2>(p)[i] < std::get<2>(p)[j];
      },
      [](P &p, size_t i, size_t j) {
        swap(get<0>(p)[i], get<0>(p)[j]);
        swap(get<1>(p)[i], get<1>(p)[j]);
        swap(get<2>(p)[i], get<2>(p)[j]);
      });

  SPDLOG_INFO("{}", t.elapsedTime());

  int32_t max_dis = 0;
  for (size_t i = 1; i < x.size(); i++) {
    int32_t min_dis = min(abs(x[i] - x[i - 1]), abs(y[i] - y[i - 1]));
    max_dis = max(max_dis, min_dis);
  }
  SPDLOG_INFO("max dis {}", max_dis);
}

TEST(System, FalseSharing) {
  // This test indicates that false sharing is not a problem for atomic_uint8_t
  const size_t N = 1000, C = 5e7;
  SPDLOG_INFO("N {} C {}", N, C);
  {
    atomic_uint8_t au[N];
    for (size_t i = 0; i < N; i++) {
      size_t d =
          reinterpret_cast<size_t>(&au[i]) - reinterpret_cast<size_t>(&au[0]);
      assert(d == i * sizeof(atomic_uint8_t));
    }
    mt19937 gen(123);
    uniform_int_distribution<size_t> dis(0, N - 1);
    Timer t;
    t.start();
#pragma omp parallel for
    for (size_t i = 0; i < C; i++) {
      size_t x = dis(gen);
      au[x].fetch_add(1, memory_order_seq_cst);
    }
    t.stop();
    SPDLOG_INFO("atomic_uint8_t Time:{} Troughtput: {} ops/ms", t.elapsedTime(),
                C / t.elapsedMs());
  }

  {
    atomic_uint64_t au[N];
    for (size_t i = 0; i < N; i++) {
      size_t d =
          reinterpret_cast<size_t>(&au[i]) - reinterpret_cast<size_t>(&au[0]);
      assert(d == i * sizeof(atomic_uint64_t));
    }
    mt19937 gen(123);
    uniform_int_distribution<size_t> dis(0, N - 1);
    Timer t;
    t.start();
#pragma omp parallel for
    for (size_t i = 0; i < C; i++) {
      size_t x = dis(gen);
      au[x].fetch_add(1, memory_order_seq_cst);
    }
    t.stop();
    SPDLOG_INFO("atomic_uint64_t Time:{} Troughtput: {} ops/ms",
                t.elapsedTime(), C / t.elapsedMs());
  }
  {
    cache_padded_size_t au[N];
    for (size_t i = 0; i < N; i++) {
      size_t d =
          reinterpret_cast<size_t>(&au[i]) - reinterpret_cast<size_t>(&au[0]);
      assert(d == i * sizeof(cache_padded_size_t));
    }
    mt19937 gen(123);
    uniform_int_distribution<size_t> dis(0, N - 1);
    Timer t;
    t.start();
#pragma omp parallel for
    for (size_t i = 0; i < C; i++) {
      size_t x = dis(gen);
      au[x].inner.fetch_add(1, memory_order_seq_cst);
    }
    t.stop();
    SPDLOG_INFO("cache_padded_size_t Time:{} Troughtput: {} ops/ms",
                t.elapsedTime(), C / t.elapsedMs());
  }
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
