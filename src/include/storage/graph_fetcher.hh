#pragma once
#include "common.hh"
#include "execution/tuple.hh"
#include "graph_store.hh"

struct VertexPropertyFetcher {
private:
  DiskArray<NoType> &ref;

public:
  TypeName vertex_type;
  ValueSchema property;

  VertexPropertyFetcher(TypeName vertex_type, ValueSchema property);

  TupleValue get_as_tv(VID id);
  void *get_as_ptr(VID id);
};

struct VertexNeighborFetcher {
private:
  VID *data;
  Offset *index;

public:
  TypeName start_type, neighbor_type, edge_type;
  Direction dir;
  VertexNeighborFetcher(TypeName start_type, TypeName neighbor_type,
                        TypeName edge_type,
                        Direction dir = Direction::Undecide);

  struct Iterator {
    VertexNeighborFetcher &fetcher;

    Offset now, end;
    Iterator(VertexNeighborFetcher &fetcher, Offset start, Offset end);
    bool next(VID &x);
  };

  Iterator get_iterator(VID id);
};