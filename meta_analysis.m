clear; clc; close all;

data = readmatrix('metrics_log.txt');

sec      = data(:,1);
nsec     = data(:,2);
commit   = data(:,3);
identity = data(:,4);
account  = data(:,5);
info     = data(:,6);
buf_pct  = data(:,7);
cpu_pct  = data(:,8);

t = (sec - sec(1)) / 3600;
msg_rate = commit + identity + account + info;
jitter_ms = nsec / 1e6;

fprintf('Rows: %d\n', numel(sec));
fprintf('Duration: %.2f hours\n', t(end));
fprintf('Jitter: max=%.3f ms, mean=%.4f ms\n', max(jitter_ms), mean(jitter_ms));
fprintf('Rate: mean=%.1f Hz, peak=%d msg/s\n', mean(msg_rate), max(msg_rate));
fprintf('Buffer peak: %.2f %%\n', max(buf_pct));
fprintf('CPU: mean=%.2f %%, max=%.2f %%\n', mean(cpu_pct), max(cpu_pct));

figure('Name','Firehose 24h Metrics','Position',[100 60 950 800]);

subplot(3,1,1);
plot(t, jitter_ms, 'LineWidth', 0.3, 'Color', [0.1 0.4 0.8]);
xlabel('Χρόνος (ώρες)');
ylabel('Jitter (ms)');
title('Διάγραμμα Jitter');
grid on;
xlim([0 24]);
yline(mean(jitter_ms), 'r--', sprintf('mean = %.3f ms', mean(jitter_ms)), ...
      'LineWidth', 1.2, 'LabelHorizontalAlignment','left');

subplot(3,1,2);
yyaxis left;
plot(t, msg_rate, 'LineWidth', 0.3);
ylabel('Πλήθος Μηνυμάτων (Hz)');
ylim([0 max(msg_rate)*1.1]);
yyaxis right;
plot(t, buf_pct, 'LineWidth', 0.3);
ylabel('Πληρότητα Buffer (%)');
ylim([0 max(buf_pct)*1.2]);
xlabel('Χρόνος (ώρες)');
title('Διάγραμμα Φόρτου και Buffer');
grid on;
xlim([0 24]);

subplot(3,1,3);
scatter(msg_rate, cpu_pct, 4, 'filled', ...
        'MarkerFaceAlpha', 0.15, 'MarkerFaceColor', [0.85 0.33 0.1]);
xlabel('Ρυθμός Μηνυμάτων (Hz)');
ylabel('Χρήση CPU (%)');
title('Διάγραμμα CPU');
grid on;
hold on;
p = polyfit(msg_rate, cpu_pct, 1);
xfit = linspace(0, max(msg_rate), 100);
plot(xfit, polyval(p, xfit), 'b-', 'LineWidth', 1.5);
legend('δείγματα', ...
       sprintf('linear fit', p(1)), ...
       'Location','northwest');
hold off;