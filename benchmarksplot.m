data = load ("benchmarks.data");


ax = plotyy (data (:,1), data (:,2), data (:,1), data (:,3));

title ("First iteration")
set(ax(1), 'ylim', [0 80000])
xlabel ("Inserted edges (Millions)");
ylabel (ax(1), "Edges-per-second");
ylabel (ax(2), "Heap-size (GigaByte)")



#print -dpsc firstIteration.jpg

print('firstIteration.png','-dpng','-r600');
