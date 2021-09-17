-ifndef(__feat__).
-define(__feat__, 42).

%% Marking some feature as `discriminator` will make featureset comparator consider two sets with different
%% `discriminator` values as _different everywhere_ which usually helps with diff readability.
-define(difference, -1).

-endif.
