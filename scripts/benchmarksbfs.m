data = load ("../benchmarkdata/bfsVsMsbfs-08-04-2016.data");

nrBreaks = 1;

h             = data(:,1)';
nrSources     = data(:,2)';
stdBfsSeconds = data(:,3)'./1000;
msbfsSeconds  = data(:,4)'./1000;

H = plot(h, stdBfsSeconds, "-", h, msbfsSeconds, "--");

title("In-2004");
set(H,'Linewidth', 1.5); 
xlabel("Max steps BFS");
ylabel("Elapsed time (Seconds)");
legend ("Standard", "Multi-source");

