data = load ('../benchmarkdata/unionVSStored-07-04-2016.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);

x  = data(:,1)';
y1 = data(:,2)'./1000;
y2 = data(:,3)'./1000;

HP(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'Unioned' );
hold on;
HP(2) = plot(x, y2, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Merged' );
hold off;

title('In-2004 $\Gamma$', 'fontsize', 16, 'interpreter', 'latex');

set(HP,'Linewidth', 2); 
xlabel ('Graph merges', 'fontsize', 16);
ylabel ('Elapsed time (s)', 'fontsize', 16);

HL = legend ({'Unioned', 'Merged'});
set(HL, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', 'StoreVsUnion', 'filename', 'StoreVsUnion.pdf');

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';

filename = '/home/johan/programming/meltwater/hyperanf-graph-prototype/scripts/StoreVsUnion.pdf';

hgexport(gcf, filename, imStyle, 'Format', 'pdf');
