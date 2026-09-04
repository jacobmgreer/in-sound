CREATE OR REPLACE MACRO get_role_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'music_department'),
        (1,  'actor'),
        (2,  'director'),
        (3,  'company'),
        (4,  'production_department'),
        (5,  'writer'),
        (6,  'camera_department'),
        (7,  'editor'),
        (8,  'miscellaneous'),
        (9,  'art_department'),
        (10, 'sound_department'),
        (11, 'visual_effects'),
        (12, 'costume_department'),
        (13, 'casting_department'),
        (14, 'producer'),
        (15, 'stunt_department'),
        (16, 'unknown')
    ) AS t(bit, value)
);
