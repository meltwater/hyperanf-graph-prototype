data = load ("../benchmarkdata/EdgeInsertionsDanfReal2016-04-08-11:51:50.data");

nrBreaks = 1;
x  = data(:,1)';
y1 = data(:,2)';
y2 = data(:,3)';
y2polf = polyfit(x, y2, nrBreaks);
y3 = data(:,4)';

[ax, h1, h2] = plotyy (x, y3, x, polyval(y2polf, x));

set(h2, "Linestyle", "--");
set(h1,'Linewidth', 1.5); 
set(h2,'Linewidth', 1.5); 
set(ax(1), 'ylim', [0 100])
set(ax(2), 'ylim', [0 10])
xlabel ("Inserted edges (Millions)");
ylabel (ax(1), "Elapsed time (Seconds)");
ylabel (ax(2), "Heap-size (GigaByte)");

legend ([h1, h2], {"Time", "Memory"}) 
