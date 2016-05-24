data = load ('../benchmarkdata/benchmarkSimTrav2016-05-19-11:08:29.data');

pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);


%ADD PLOT

x  = data(:,1)./1000000; %BULK SIZE
y1 = data(:,2)./1000; %SIMULATED TIME S
y2 = data(:,5)./1000; %TRAVERSE TIME S

subplot(1,2,1);
HP(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'High level' );
hold on;
HP(2) = plot(x, y2, 'x-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Byte stream' );
hold off;

set(HP,'Linewidth', 2);    
xlabel ('Inserted edges (Millions)', 'fontsize', 16);
ylabel ('Add time (s)', 'fontsize', 16);
xlim([0,3]);
ylim([0,160]);

HL = legend (HP);
set(HL, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);



%ITERATE PLOT
y1 = data(:,3)./1000; %SIMULATED TIME MS
y2 = data(:,6)./1000; %TRAVERSE TIME MS

subplot(1,2,2);
HP(1) = plot(x, y1, 'd-', 'color', [0, 0, 0.5],'markersize', 4, 'markerfacecolor', [0, 0, 0.5], 'displayname', 'High level' );
hold on;
HP(2) = plot(x, y2, 'x-', 'color', [0.5, 0, 0],'markersize', 4, 'markerfacecolor', [0.5, 0, 0], 'displayname', 'Byte stream' );
hold off;

set(HP,'Linewidth', 2);    
xlabel ('Added edges (Millions)', 'fontsize', 16);
ylabel ('Iterate time (s)', 'fontsize', 16);
xlim([0,3]);
ylim([0,550]);

HL = legend (HP);
set(HL, 'fontsize', 16, 'location', 'northwest');

set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);


% PRINT PDF

filename = 'benchmarkDanfVsTrivial.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

