data = load ('../files/DANFComparedToTrivial2016-05-16-10:43:13.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%TIME PLOT

x  = data(:,1); %BULK SIZE
y1 = data(:,2); %DANF EPS
y2 = data(:,7); %TRIVIAL EPS

sp1 = subplot(1,2,1);
HP1(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'DANF' );
hold on;
HP1(2) = plot(x, y2, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Trivial' );
hold off;

set(HP1,'Linewidth', 2); 
ylim([0, 4100]);
xlim([0,6400]);
ax = gca;
ax.XTick = [0, 3200, 6400];
xlabel ('Bulk size', 'fontsize', 16);
ylabel ('EPS', 'fontsize', 16);

HL1 = legend (HP1);
set(HL1, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);



%MEMORY PLOT

x  = data(:,1); %BULK SIZE
y1 = data(:,3); %DANF GRAPH MEM
y2 = data(:,4); %DANF COUNTER MEM
y3 = data(:,5); %DANF VC MEM
y4 = data(:,6); %DANF MSBFS MEM
y5 = data(:,8); %TRIVIAL MEM

subplot(1,2,2);
HP2 = area(x, [y1,y2,y3,y4]);
set(HP2, {'displayname'}, {'Graph';'Counters';'VC';'MSBFS'} );
hold on;
HP2(5) = plot(x, y5, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Trivial' );
hold off;

set(HP2,'Linewidth', 2); 
ylim([0, 0.6]);
xlim([0,6400]);
ax = gca;
ax.XTick = [0, 3200, 6400];
xlabel ('Bulk size', 'fontsize', 16);
ylabel ('Memory Usage (GB)', 'fontsize', 16);

HL2 = legend (HP2);
set(HL2, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


%BOTH
set(gcf,'NextPlot','add');
axes;
h = title('It-2004' , 'fontsize', 16, 'interpreter', 'latex');
set(gca,'Visible','off');
set(h,'Visible','on'); 


% PRINT PDF

filename = 'BenchmarkDanfVsTrivial.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');
