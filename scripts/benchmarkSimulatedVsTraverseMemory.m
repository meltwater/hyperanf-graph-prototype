data = load ('../benchmarkdata/benchmarkSimTrav2016-05-19-11:08:29.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%ADD PLOT

x  = data(:,1)./1000000; %BULK SIZE
y1 = data(:,4); %SIMULATED MEM GB
y2 = data(:,7); %TRAVERSE MEM GB

HP(1) = plot(x, y1, 'd-', 'color', [0.5, 0, 0],'markersize', 6, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'High level' );
grid on;
hold on;
HP(2) = plot(x, y2, 'p-', 'color', [0, 0.5, 0],'markersize', 6, 'markerfacecolor', [0, 0.5, 0], 'displayname', 'Byte stream' );
hold off;

set(HP,'Linewidth', 2);    
xlabel ('Edges (Millions)', 'fontsize', 16);
ylabel ('Memory usage (GB)', 'fontsize', 16);
ylim([0, 0.5]);

HL = legend (HP);
set(HL, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


% PRINT PDF

filename = 'benchmarkByteStreamVsHighLevelMemory.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

