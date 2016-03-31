data = load ("benchmarks.data");

ax = plotyy (data (:,1), data (:,2), data (:,1), data (:,3));
title ("First iteration")
xlabel ("Insertions");
ylabel (ax(1), "DPS");
ylabel (ax(2), "Heap-size")
