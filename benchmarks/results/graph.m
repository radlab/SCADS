#!/usr/bin/octave -q

times = {
  { 
    [27700676, 30.000001, 34637238, 30.000001],
    [27842304, 30.000002, 34558638, 30.000001],
    [10436369, 30.000001, 3050012, 30.000001]
  },
  {
    [5523716, 30.000005, 2392933, 30.000012], 
    [6297557, 30.000002, 3085063, 30.000007],
    [491590, 30.00038, 298119, 30.000068]
  },
  {
    [11060126, 30.000002, 22729446, 30.000001],
    [11131233, 30.000003, 22624197, 30.000001],
    [5836067, 30.000004, 2019126, 30.000011] 
  },
  {
    [25991900, 30.000001, 12031903, 30.000001],
    [25851875, 30.000001, 12271551, 30.000002],
    [5779132, 30.000003, 1126360, 30.000017]
  },
  {
    [2198238, 30.000002, 890014, 30.000031],
    [2285335, 30.000011, 986417, 30.000018],
    [311811, 30.000060, 276567, 30.000042]
  }
};

names = {
  { 'Primitive Record', 'primitives' },
  { 'Sequence Of Primitives Record', 'seqprimitives' },
  { 'Byte Container Record', 'bytecontainer' },
  { 'Message Container Record', 'messagecontainer' },
  { 'Record List Record', 'recordlist' }
};

for i=[1:length(times)]
  vec = times{i};

  name = names{i};

  plugin = vec{1};
  stock  = vec{2};
  java   = vec{3};

  hold on;

  barh([plugin(1)/plugin(2), plugin(3)/plugin(4);
        stock(1)/stock(2), stock(3)/stock(4);
        java(1)/java(2), java(3)/java(4) ], 0.3, 'grouped');

  title(sprintf('Benchmark Results for %s', name{1}), 'fontsize', 16);
  legend({ 'Serialization', 'Deserialization' }, 'location', 'southeast');
  xlabel('Messages/Second', 'fontsize', 16);
  ylabel('Record Type', 'fontsize', 16);
  set(gca, 'ytick', 1:1:3);
  set(gca, 'yticklabel', { 'Plugin', 'Stock', 'Java' });
  set(gca, 'fontsize', 16);

  print(sprintf('%s.png', name{2}), '-dpng');
  print(sprintf('%s.pdf', name{2}), '-dpdf');

  close all;

end
