data = load ('../benchmarkdata/DANFComparedToTrivial2016-05-24-10:18:44.data'); %h = 3
data =load('../benchmarkdata/DANFComparedToTrivial2016-05-17-09:48:41.data'); %h = 8

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%TIME PLOT

x  = data(:,1); %BULK SIZE
y1 = data(:,2); %DANF EPS
y2 = data(:,7); %TRIVIAL EPS

sp1 = subplot(1,2,1);
HP1(1) = plot(x, y1, 'o-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'DANF' );
grid on;
hold on;
HP1(2) = plot(x, y2, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Two-BFS' );
hold off;

set(HP1,'Linewidth', 2);    
ylim([0, 250]);
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

map = [
    0, 0, 0  ;
    0, 1.0, 0;
    0, 0, 1.0;
    1.0, 0, 0;
    
];
cmap = colormap(map);

subplot(1,2,2);
HP2 = area(x, [y1,y2,y3,y4]);
set(HP2, {'displayname'}, {'Graph';'Counters';'VC';'MS-BFS'} );

color = zeros(4,3);
for i = 1:4
    index = i * (length(cmap) / 4);
    cmap(index,:);
    color(i,:) = cmap(index,:);
end
 set(HP2, {'facecolor'}, {color(1,:); color(2,:); color(3,:); color(4,:) } );
    
hold on;
HP2(5) = plot(x, y5, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Two-BFS' );
grid on;
hold off;

set(HP2,'linestyle', 'none'); 
set(HP2(5), 'linestyle', '-', 'linewidth', 2);
ylim([0, 2.8]);
xlim([0,6400]);
xlabel('Bulk size');
ax = gca;
ax.XTick = [0, 3200, 6400];
xlabel ('Bulk size', 'fontsize', 16);
ylabel ('Memory Usage (GB)', 'fontsize', 16);

HL2 = legend (HP2);
set(HL2, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


%BOTH
%set(gcf,'NextPlot','add');
%axes;
%h = title('In-2004' , 'fontsize', 16, 'interpreter', 'latex');
%set(gca,'Visible','off');
%set(h,'Visible','on'); 


% PRINT PDF

filename = 'benchmarkDanfVsTrivial.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

