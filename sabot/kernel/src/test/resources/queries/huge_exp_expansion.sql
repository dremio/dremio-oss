WITH
  my_table AS (
    SELECT
      c1,
      c2
    FROM
      (
        VALUES
          ('1', '1'),
          ('2', '2'),
          ('3', '3'),
          ('4', '4'),
          ('5', '5')
      ) AS t1 (c1, c2)
  ),
  input_layer_0 AS (
    SELECT
      CASE
        WHEN (
          (source_table.c1 <> 'const5')
          AND NULLIF(
            (
              source_table.c2 IN ('const6', 'const7', 'const8', 'const9')
            ),
            FALSE
          )
        ) THEN 'const9'
        WHEN (source_table.c1 = 'const5') THEN 'const10'
        WHEN (
          CASE
            WHEN CHAR_LENGTH('const11') = 0 THEN 1
            ELSE POSITION('const11' in source_table.c2)
          END > 0
        ) THEN 'const12'
        WHEN (
          CASE
            WHEN CHAR_LENGTH('const12') = 0 THEN 1
            ELSE POSITION('const12' in source_table.c2)
          END > 0
        ) THEN 'const12'
        WHEN (
          CASE
            WHEN CHAR_LENGTH('const13') = 0 THEN 1
            ELSE POSITION('const13' in source_table.c2)
          END > 0
        ) THEN 'const14'
        WHEN NULLIF(
          (
            source_table.c2 IN (
              'const15',
              'const16',
              'const17',
              'const18',
              'const19',
              'const20',
              'const21'
            )
          ),
          FALSE
        ) THEN 'const22'
        WHEN (
          (source_table.c1 = 'const23')
          AND (source_table.c2 <> 'const24')
        ) THEN 'const25'
        WHEN NULLIF(
          (
            source_table.c1 IN (
              'const26',
              'const27',
              'const28',
              'const29',
              'const30',
              'const31',
              'const32',
              'const33',
              'const34',
              'const35',
              'const36',
              'const37',
              'const38',
              'const39'
            )
          ),
          FALSE
        ) THEN 'const40'
        WHEN NULLIF(
          (
            source_table.c2 IN ('const41', 'const42', 'const43')
          ),
          FALSE
        ) THEN 'const44'
        WHEN (source_table.c1 = 'const45') THEN 'const45'
        WHEN NULLIF(
          (
            source_table.c2 IN ('const41', 'const42', 'const43')
          ),
          FALSE
        ) THEN 'const44'
        ELSE source_table.c2
      END as input_0
    FROM
      my_table AS source_table
  ),
  input_layer_1 AS (
    SELECT
      CASE (input_0)
        WHEN 'const9' THEN 'const9'
        WHEN 'const6' THEN 'const9'
        WHEN 'const46' THEN 'const46'
        WHEN 'const47' THEN 'const47'
        WHEN 'const48' THEN 'const47'
        WHEN 'const49' THEN 'const47'
        WHEN 'const44' THEN 'const44'
        WHEN 'const50' THEN 'const50'
        WHEN 'const12' THEN 'const12'
        WHEN 'const15' THEN 'const15'
        WHEN 'const22' THEN 'const15'
        WHEN 'const16' THEN 'const15'
        WHEN 'const17' THEN 'const15'
        WHEN 'const18' THEN 'const15'
        WHEN 'const19' THEN 'const15'
        WHEN 'const20' THEN 'const15'
        WHEN 'const21' THEN 'const15'
        WHEN 'const51' THEN 'const51'
        WHEN 'const52' THEN 'const53'
        WHEN 'const54' THEN 'const53'
        WHEN 'const55' THEN 'const53'
        WHEN 'const56' THEN 'const53'
        WHEN 'const57' THEN 'const53'
        WHEN 'const58' THEN 'const53'
        WHEN 'const59' THEN 'const53'
        WHEN 'const60' THEN 'const53'
        WHEN 'const61' THEN 'const53'
        WHEN 'const62' THEN 'const63'
        WHEN 'const64' THEN 'const64'
        WHEN 'const65' THEN 'const65'
        WHEN 'const66' THEN 'const66'
        WHEN 'const10' THEN 'const10'
        WHEN 'const67' THEN 'const68'
        WHEN 'const69' THEN 'const68'
        WHEN 'const70' THEN 'const68'
        WHEN 'const71' THEN 'const68'
        WHEN 'const72' THEN 'const68'
        WHEN 'const73' THEN 'const68'
        WHEN 'const14' THEN 'const14'
        WHEN 'const74' THEN 'const74'
        WHEN 'const75' THEN 'const76'
        WHEN 'const77' THEN 'const78'
        WHEN 'const79' THEN 'const78'
        WHEN 'const80' THEN 'const78'
        WHEN 'const81' THEN 'const78'
        WHEN 'const82' THEN 'const78'
        WHEN 'const45' THEN 'const78'
        WHEN 'const83' THEN 'const78'
        WHEN 'const84' THEN 'const78'
        WHEN 'const85' THEN 'const78'
        WHEN 'const86' THEN 'const78'
        WHEN 'const87' THEN 'const78'
        WHEN 'const88' THEN 'const89'
        WHEN 'const90' THEN 'const90'
        WHEN 'const91' THEN 'const91'
        WHEN 'const92' THEN 'const92'
        WHEN 'const93' THEN 'const93'
        WHEN 'const94' THEN 'const93'
        WHEN 'const95' THEN 'const96'
        WHEN 'const97' THEN 'const96'
        WHEN 'const98' THEN 'const98'
        WHEN 'const99' THEN 'const98'
        WHEN 'const100' THEN 'const25'
        WHEN 'const101' THEN 'const25'
        WHEN 'const25' THEN 'const25'
        WHEN 'const102' THEN 'const25'
        WHEN 'const103' THEN 'const104'
        WHEN 'const105' THEN 'const104'
        WHEN 'const104' THEN 'const104'
        WHEN 'const106' THEN 'const107'
        WHEN 'const108' THEN 'const107'
        WHEN 'const109' THEN 'const107'
        WHEN 'const110' THEN 'const107'
        WHEN 'const111' THEN 'const107'
        WHEN 'const112' THEN 'const107'
        WHEN 'const113' THEN 'const107'
        WHEN 'const114' THEN 'const107'
        WHEN 'const115' THEN 'const107'
        WHEN 'const40' THEN 'const107'
        WHEN 'const36' THEN 'const107'
        WHEN 'const116' THEN 'const107'
        WHEN 'const117' THEN 'const107'
        WHEN 'const118' THEN 'const107'
        WHEN 'const119' THEN 'const120'
        WHEN 'const121' THEN 'const121'
        WHEN 'const122' THEN 'const122'
        WHEN 'const123' THEN 'const123'
        WHEN 'const124' THEN 'const124'
        WHEN 'const24' THEN 'const125'
        WHEN 'const126' THEN 'const127'
        WHEN 'const128' THEN 'const127'
        WHEN 'const129' THEN 'const127'
        WHEN 'const130' THEN 'const131'
        WHEN 'const132' THEN 'const131'
        WHEN 'const133' THEN 'const133'
        WHEN 'const134' THEN 'const135'
        WHEN 'const136' THEN 'const135'
        WHEN 'const137' THEN 'const138'
        WHEN 'const139' THEN 'const138'
        WHEN 'const140' THEN 'const140'
        ELSE (input_0)
      END as input_1
    FROM
      input_layer_0
  ),
  input_layer_2 as (
    SELECT
      (
        CASE
          WHEN NULLIF(
            (
              (input_1) IN (
                'const10',
                'const131',
                'const15',
                'const78',
                'const138',
                'const25',
                'const98',
                'const127',
                'const135',
                'const64',
                'const107',
                'const90',
                'const123'
              )
            ),
            FALSE
          ) THEN 'const141'
          WHEN ((input_1) = 'const121') THEN 'const142'
          WHEN NULLIF(
            (
              (input_1) IN (
                'const14',
                'const12',
                'const47',
                'const122',
                'const68',
                'const9',
                'const125'
              )
            ),
            FALSE
          ) THEN 'const143'
          WHEN ((input_1) = 'const124') THEN 'const144'
          WHEN NULLIF(
            (
              (input_1) IN ('const145', 'const89', 'const120', 'const140')
            ),
            FALSE
          ) THEN 'const146'
          ELSE 'const147'
        END
      ) AS input_2
    from
      input_layer_1
  )
SELECT
  input_2
FROM
  input_layer_2
GROUP BY
  input_2
ORDER BY
  input_2 ASC NULLS FIRST