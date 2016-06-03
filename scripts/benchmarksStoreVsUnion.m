data = load ('../benchmarkdata/unionVSStored2016-05-30-10:43:58.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%TIME PLOT
x  = data(:,1); %BULK INSERTIONS
y1 = data(:,5)./1000; %ALWAYS TIME
y2 = data(:,7)./1000; %SOMETIMES TIME
y3 = data(:,9)./1000;%NEVER TIME

subplot(1,2,1);
HP1(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'r = \infty' );
grid on;
hold on;
HP1(2) = plot(x, y2, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'r = 8.0' );
hold on;
HP1(3) = plot(x, y3, 'p-', 'color', [0, 0.5, 0],'markersize', 4, 'markerfacecolor', [0, 0.5, 0], 'displayname', 'r = 0.0' );
hold off;

ylim([0, 5.5]);

set(HP1,'Linewidth', 2);    
xlabel ('#Bulk insertions', 'fontsize', 16);
ylabel ('Average elapsed time (s)', 'fontsize', 16);

HL1 = legend (HP1);
set(HL1, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);



%MEMORY PLOT
y1 = data(:,6) .* 1024; %ALWAYS MEM
y2 = data(:,8) .* 1024; %SOMETIMES MEM
y3 = data(:,10).* 1024;%NEVER MEM

subplot(1,2,2);
HP2(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'r = \infty' );
grid on;
hold on;
HP2(2) = plot(x, y2, 's-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'r = 8.0' );
hold on;
HP2(3) = plot(x, y3, 'p-', 'color', [0, 0.5, 0],'markersize', 4, 'markerfacecolor', [0, 0.5, 0], 'displayname', 'r = 0.0' );
hold off;

ylim([0, 230]);

set(HP2,'Linewidth', 2);    
xlabel ('#Bulk insertions', 'fontsize', 16);
ylabel ('Memory usage (MB)', 'fontsize', 16);

HL2 = legend (HP2);
set(HL2, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


% PRINT PDF

filename = 'benchmarkUnionVsStored.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

