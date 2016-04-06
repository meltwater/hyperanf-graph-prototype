data = load ("../unionVSStored.data");

x  = data(:,1)';
y1 = data(:,2)'./1000;
y2 = data(:,3)'./1000;

h = plot(x, y1, "-", x, y2, "--");

title("In-2004");

set(h,'Linewidth', 1.5); 
xlabel ("Graph merges");
ylabel ("Elapsed time (Seconds)");

legend ({"Unioned", "Merged"}) 
