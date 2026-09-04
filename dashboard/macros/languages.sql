CREATE OR REPLACE MACRO get_language_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'english'),
        (1,  'germanic'),
        (2,  'francophone'),
        (3,  'hispanophone'),
        (4,  'subcontinental'),
        (5,  'silent'),
        (6,  'japanese'),
        (7,  'sinophone'),
        (8,  'nordic'),
        (9,  'italophone'),
        (10, 'slavic_e'),
        (11, 'lusophone'),
        (12, 'slavic_w'),
        (13, 'asia_pacific'),
        (14, 'european_other'),
        (15, 'arabic'),
        (16, 'korean'),
        (17, 'middle_eastern'),
        (18, 'turkish'),
        (19, 'romance_other'),
        (20, 'african'),
        (21, 'special_language'),
        (22, 'indigenous'),
        (23, 'caribbean_creole')
    ) AS t(bit, value)
);
