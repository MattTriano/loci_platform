from dataclasses import dataclass

from loci.exports.geojson_export import GeoJSONExportConfig

# Allowed formatter names. The frontend has matching JS implementations.
# Keep this list small and explicit so the frontend doesn't have to
# evaluate arbitrary code; new formatters are added in both places.
ALLOWED_FORMATTERS = frozenset(
    {
        "yes_no_yn",  # 'Y'/'N' -> 'Yes'/'No'
        "yes_no_bool",  # True/False or 'yes'/'no' -> 'Yes'/'No'
        "hour",  # 14 -> '14:00'
        "street_with_direction",  # uses {street_direction} {street_name} from props
    }
)


@dataclass(frozen=True)
class PopupField:
    """One row in a layer's popup.

    key
        Property name. Must exist in the export's properties list, OR be
        the dependent field of a multi-field formatter (see notes below).
    label
        Display label, e.g. "Worst Injury".
    fmt
        Optional named formatter from ALLOWED_FORMATTERS.

    Notes on multi-field formatters:
        Some formatters read more than one property (e.g.
        street_with_direction reads `street_name` and `street_direction`).
        For those, set `key` to the primary field that drives whether the
        row appears at all; the formatter pulls the rest from the feature.
    """

    key: str
    label: str
    fmt: str | None = None


@dataclass(frozen=True)
class LayerDisplayConfig:
    """How the bike-map should display one exported layer.

    export_name
        Must match a GeoJSONExportConfig.name in the same CityBuildSpec.
    label
        Layer toggle label, e.g. "Bike Crashes".
    color
        Hex color used for the toggle swatch, points, and clusters.
    popup_title
        Popup heading, e.g. "Bike Crash".
    popup_fields
        Ordered list of fields to show in the popup. Fields whose value
        is null/empty in a given feature are skipped at render time.
    date_field
        If set, the frontend renders a date-range filter for this layer
        and the property must exist in the export's properties list.
    default_visible
        If True, layer loads on first render.
    cluster_max_zoom, cluster_radius, point_radius
        Map render tuning. Defaults match the existing Chicago layers.
    """

    export_name: str
    label: str
    color: str
    popup_title: str
    popup_fields: list[PopupField]
    date_field: str | None = None
    default_visible: bool = True
    cluster_max_zoom: int = 13
    cluster_radius: int = 45
    point_radius: int = 5


def validate_layer_displays(
    layer_displays: list[LayerDisplayConfig],
    geojson_exports: list[GeoJSONExportConfig],
) -> None:
    """Validate that each LayerDisplayConfig pairs with a real export.

    Checks:
      - export_name matches a GeoJSONExportConfig.name
      - date_field, if set, exists in that export's properties
      - each PopupField.fmt, if set, is in ALLOWED_FORMATTERS

    Note: PopupField.key is NOT checked against properties because
    multi-field formatters (e.g. street_with_direction) may name a
    primary key that's still in properties while pulling other fields
    from the feature dynamically. We rely on the formatter set being
    explicit and small.

    Raises ValueError on the first problem found. Intended to run at
    DAG-parse time via CityBuildSpec.__post_init__.
    """
    exports_by_name = {e.name: e for e in geojson_exports}

    for ld in layer_displays:
        if ld.export_name not in exports_by_name:
            raise ValueError(
                f"LayerDisplayConfig {ld.export_name!r}: "
                f"no GeoJSONExportConfig with that name "
                f"(available: {sorted(exports_by_name)})"
            )

        export_props = set(exports_by_name[ld.export_name].properties or [])

        if ld.date_field is not None and ld.date_field not in export_props:
            raise ValueError(
                f"LayerDisplayConfig {ld.export_name!r}: "
                f"date_field {ld.date_field!r} not in export properties "
                f"(available: {sorted(export_props)})"
            )

        for pf in ld.popup_fields:
            if pf.fmt is not None and pf.fmt not in ALLOWED_FORMATTERS:
                raise ValueError(
                    f"LayerDisplayConfig {ld.export_name!r}, popup field "
                    f"{pf.key!r}: unknown formatter {pf.fmt!r} "
                    f"(allowed: {sorted(ALLOWED_FORMATTERS)})"
                )
