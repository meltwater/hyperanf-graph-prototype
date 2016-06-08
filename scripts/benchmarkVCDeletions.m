data = load ('../benchmarkdata/DvcDeletionsReal2016-05-31-14:27:08.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);



x  = data(:,1); % MODIFICATIONS
y1 = data(:,4); % ELAPSED TIME

HP(1) = plot(x, y1, 'd-', 'color', [0.5, 0, 0],'markersize', 6, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Time' );
grid on;
hold off;

ylim([0, 250]);
xlim([0, 1100]);

ylabel ('Elapsed time (s)', 'fontsize', 16);
xlabel ('Deleted edges (Millions)', 'fontsize', 16);

set(HP,'Linewidth', 2);    

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


% PRINT PDF

filename = 'benchmarkDvcDeletions.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

