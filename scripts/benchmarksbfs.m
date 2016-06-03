data = sortrows(load ('../benchmarkdata/benchmarkBfs2016-05-31-16:43:26.data'), 2);

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%ADD PLOT
x  = sort(unique(data(:,1))); % h


y = data(:,3) ./ data(:,4); 

cmap = hsv;

markers = ['-d';'-s';'-x';'-c';'-p'];

grid on;
for index = 1:5
    colorIndex = ceil(index * (length(cmap) / 5));
    color = cmap(colorIndex,:);
    
    yIndex = data(:,2)==(index*1000);
    yCurrent = y(yIndex,:);
    
    HP(index) = semilogy(x, yCurrent, markers(index,:), 'color', color ,'markersize', 6, 'markerfacecolor', color, 'displayname', num2str(index * 1000) );
    hold on;
end

semilogy(x, repmat([1], 1, length(x)), '--k');
hold off;


set(HP,'Linewidth', 2);    
xlabel ('Max steps', 'fontsize', 16);
ylabel ('BFS/MS-BFS Ratio', 'fontsize', 16);

HL = legend (HP);
set(HL, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


% PRINT PDF
filename = 'benchmarkBfs.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';

hgexport(gcf, filename, imStyle, 'Format', 'pdf');

