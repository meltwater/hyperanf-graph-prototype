pageWidth  = 426.79135;
pageHeight = pageWidth / sqrt(2);

Hfig = figure(1);

        %m n added edges
data = [ %H = 3, log2m = 4
        72172 ,    10617, 120000; %h3 wordassociation-2011       10617
      3050615 ,   100000,  15000; %h3 uk-2007-05@100000           100000
      5158388 ,   735323,  60000; %h3 amazon-2008                 735323
     16917053 ,  1382908,  73000; %h3 in-2004                     1382908
     79023142 ,  5363260,  30000; %h3 ljournal-2008               5363260
    194109311 ,  7414866,  45000; %h3 indochina-2004              7414866
    386915963 , 11264052,  22000; %h? eu-2015-host                11264052
    %602119716 ,30809122,  14000; %h? gsh-2015-tpd               30809122
    639999458 , 22744080,  65000; %h3 arabic-2005                 22744080
    936364282 , 39459925,  70000; %h? uk-2005                     39459925
    1019903190,118142155,  70000; %h? Webbase-2001                118142155
    1150725436, 41291594,  65000; %h3 it-2004                     41291594
];

y = data(:,3) ./ data(:,1);              % forming y of X.beta = y

x = data(:,1) ./ 1000000;

xx = x';

m = length(data(:,1));
n = length(2);

X = ones(m, n);              % forming X of X.beta = y
X(:,2) = x;

betaHat = (X' * X) \ X' * y; % computing projection of matrix X on y, giving beta

yy = betaHat(1) + betaHat(2)*xx;


%HP(1) = semilogy(xx, yy, '-', 'color', [0, 0, 0.5], 'displayname', 'Least square' );
%hold on
HP = semilogy(xx, y, '-xr', 'markersize', 8 , 'displayname', 'Sampled ratio');
xlim([-100, 1200]);
grid on;
hold off

set(HP,'Linewidth', 2);    
xlabel ('Graph edges (Millions)', 'fontsize', 16);
ylabel ('Added / Total edges', 'fontsize', 16);

ax = gca;
%set(gca, 'YTickLabel', { 0.000001, 0.0001, 0.01, 1, 100});


set(gca, 'fontsize', 14 );
set(gca, 'ticklength', [0.02, 0.05]);



% PRINT PDF

filename = 'benchmarkDanfVsHanf.pdf';

set(Hfig , 'units', 'points', 'paperunits', 'points', 'paperposition',  [0, 0, pageWidth, pageHeight], 'papersize', [pageWidth, pageHeight], 'position', [0, 0, pageWidth, pageHeight], 'name', filename, 'filename', filename);

imStyle = hgexport('factorystyle');

imStyle.Format = 'pdf';
imStyle.Width = pageWidth;
imStyle.Height = pageHeight;
imStyle.Units = 'points';


hgexport(gcf, filename, imStyle, 'Format', 'pdf');

