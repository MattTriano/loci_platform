-- macros/road_infra_stress_matrix.sql
-- Per-meter stress factor combining road class and bike infrastructure.
--
-- Replaces the old multiplicative model:
--   stress = road_type_factor * infrastructure_factor
-- with a single lookup matrix because those two factors are NOT
-- independent. The effect of bike infrastructure depends heavily on
-- what road it sits next to — a protected lane on a 45 mph primary
-- is a very different experience than the same lane on a residential
-- street.
--
-- Values grounded in:
--   - LTS framework (Mekuria, Furth, Nixon 2012) for structure
--   - Teschke et al. (2012) case-crossover injury study for relative
--     magnitudes (cycle track ≈ 0.11× risk of worst-case reference;
--     bike lane ≈ 0.54×; local street ≈ 0.51×)
--
-- Scale convention: ~1.0 = plain residential street (no bike infra),
-- which serves as a middle reference. Higher = more stress per meter.
--
-- Unspecified (road_class, infra_type) combinations fall through to
-- the "no infra" factor for that road class. The validator flags
-- implausible combinations separately.
--
-- Args:
--   highway_col: column expression for OSM highway tag
--   service_col: column expression for OSM service tag
--   infra_col:   column expression for derived osm_infra_type

{% macro road_infra_stress_factor(highway_col, service_col, infra_col) %}
    -- Expects highway_col to be a resolved single value (no stringified
    -- lists). Staging model stg__osmnx_chicago_bike_network_edges handles
    -- that. If a compound value slips through, it will hit the `else 2.70`
    -- fallback, which the validator's check_b_classifier_disagreement
    -- should flag separately.
case
    -- ===================================================================
    -- highway=cycleway: the way IS the infrastructure. Treat uniformly
    -- regardless of any cycleway_* sub-tags.
    -- ===================================================================
    when {{ highway_col }} = 'cycleway'
      or {{ highway_col }} like '%cycleway%'
        then 0.20

    -- ===================================================================
    -- Shared-use paths (park paths, greenways joined via footway/path
    -- with bicycle=permissive/yes/designated)
    -- ===================================================================
    when {{ highway_col }} in ('path', 'footway', 'pedestrian', 'bridleway')
        then case
            when {{ infra_col }} = 'designated_path' then 0.55
            else                                          0.45
        end

    -- ===================================================================
    -- service roads: penalty by service subtype.
    -- Bike infra on service roads is rare; if present, we still want
    -- to strongly discourage alleys and parking aisles because users
    -- find them weird and popped-tire risk is real.
    -- ===================================================================
    when {{ highway_col }} = 'service'
        then case
            when {{ service_col }} = 'alley'         then 2.50
            when {{ service_col }} = 'parking_aisle' then 3.50
            else                                          1.10
        end

    -- ===================================================================
    -- Road-class × infra-type matrix for the main drivable road hierarchy.
    --
    -- Columns (infra_type):
    --   track            = physically separated cycletrack (curb/planter/etc)
    --   protected_lane   = lane with documented separation
    --   buffered_lane    = lane with painted buffer
    --   bike_lane        = plain painted lane
    --   sharrow          = sharrow markings or share_busway
    --   bicycle_road     = bicycle-priority road (local streets only)
    --   (none)           = no bike infra
    -- ===================================================================

    -- living_street
    when {{ highway_col }} in ('living_street')
        then case {{ infra_col }}
            when 'track'          then 0.25
            when 'protected_lane' then 0.30
            when 'buffered_lane'  then 0.45
            when 'bike_lane'      then 0.55
            when 'sharrow'        then 0.75
            when 'share_busway'   then 0.75
            when 'bicycle_road'   then 0.70
            else                       0.85
        end

    -- residential (and any compound highway value containing 'residential')
    when {{ highway_col }} = 'residential'
      or {{ highway_col }} like '%residential%'
        then case {{ infra_col }}
            when 'track'          then 0.30
            when 'protected_lane' then 0.40
            when 'buffered_lane'  then 0.60
            when 'bike_lane'      then 0.75
            when 'sharrow'        then 0.95
            when 'share_busway'   then 0.95
            when 'bicycle_road'   then 0.80
            else                       1.00
        end

    -- unclassified
    when {{ highway_col }} = 'unclassified'
        then case {{ infra_col }}
            when 'track'          then 0.35
            when 'protected_lane' then 0.45
            when 'buffered_lane'  then 0.70
            when 'bike_lane'      then 0.90
            when 'sharrow'        then 1.15
            when 'share_busway'   then 1.15
            when 'bicycle_road'   then 0.95
            else                       1.30
        end

    -- tertiary
    when {{ highway_col }} in ('tertiary', 'tertiary_link')
      or {{ highway_col }} like '%tertiary%'
        then case {{ infra_col }}
            when 'track'          then 0.35
            when 'protected_lane' then 0.50
            when 'buffered_lane'  then 0.95
            when 'bike_lane'      then 1.25
            when 'sharrow'        then 1.60
            when 'share_busway'   then 1.60
            else                       1.80
        end

    -- secondary
    when {{ highway_col }} in ('secondary', 'secondary_link')
      or {{ highway_col }} like '%secondary%'
        then case {{ infra_col }}
            when 'track'          then 0.40
            when 'protected_lane' then 0.60
            when 'buffered_lane'  then 1.15
            when 'bike_lane'      then 1.50
            when 'sharrow'        then 2.00
            when 'share_busway'   then 2.00
            else                       2.30
        end

    -- primary
    when {{ highway_col }} in ('primary', 'primary_link')
      or {{ highway_col }} like '%primary%'
        then case {{ infra_col }}
            when 'track'          then 0.45
            when 'protected_lane' then 0.70
            when 'buffered_lane'  then 1.40
            when 'bike_lane'      then 1.75
            when 'sharrow'        then 2.40
            when 'share_busway'   then 2.40
            else                       2.70
        end

    -- busway (shared with buses — not great, not terrible)
    when {{ highway_col }} = 'busway'
        then 1.40

    -- Typically unpaved; surface_factor catches part of this but many
    -- are tagged with surface=null. Treat as slightly worse than
    -- residential to reflect expected poor surface and remote location
    -- (less lighting, less crowd safety-in-numbers).
    when {{ highway_col }} = 'track'
        then case {{ infra_col }}
            when 'track'          then 0.50
            when 'protected_lane' then 0.60
            when 'buffered_lane'  then 0.90
            when 'bike_lane'      then 1.10
            when 'sharrow'        then 1.30
            when 'share_busway'   then 1.30
            when 'bicycle_road'   then 1.15
            else                       1.40
        end

    -- Fallback for anything else (trunk, motorway, etc. shouldn't be
    -- in the bike network, but if they slip through, score as primary
    -- + no infra to be safe).
    else 2.70
end
{% endmacro %}
