"""
Builder for Overpass QL queries.

An OverpassAPIQuery describes *what* to fetch from the Overpass API in a
declarative way: which OSM element types, which tag filters, what spatial
extent, and what output verbosity. The .to_ql() method renders it into a
valid Overpass QL string.

Example:
    query = OverpassAPIQuery(
        element_types=["node", "way"],
        tag_filters=[{"amenity": "cafe"}],
        area_name="Chicago",
    )
    print(query.to_ql())

    [out:json][timeout:180];
    area["name"="Chicago"]->.searchArea;
    (
      node["amenity"="cafe"](area.searchArea);
      way["amenity"="cafe"](area.searchArea);
    );
    out geom meta;
"""

from __future__ import annotations

from dataclasses import dataclass

VALID_ELEMENT_TYPES = {"node", "way", "relation"}
VALID_OUT_MODES = {"geom", "center"}


@dataclass(frozen=True)
class Regex:
    """
    Marker type for a raw regex pattern in a tag filter value.

    Wrap a regex pattern in this when you want it passed through to
    Overpass without escaping. Useful for prefix matches, alternation,
    etc.

    Note: Overpass uses POSIX ERE regex syntax, which does NOT support
    Perl-style inline flags like `(?i)`. For case-insensitive matching,
    set case_insensitive=True instead — this renders Overpass's
    `,i` filter modifier.

    Examples:
        tag_filters=[{"name": Regex("cafe", case_insensitive=True)}]
        tag_filters=[{"highway": Regex("^primary")}]
    """

    pattern: str
    case_insensitive: bool = False


@dataclass
class OverpassAPIQuery:
    """
    Declarative representation of an Overpass API query.

    Parameters
    ----------
    element_types : list[str]
        Which OSM element types to fetch. Non-empty subset of
        {"node", "way", "relation"}.
    tag_filters : list[dict[str, str | list[str] | Regex | None]]
        List of filter groups. Each dict is one OR-branch; its entries
        are AND'd within. Value semantics:
            None         -> key exists with any value
            str          -> exact match
            list[str]    -> any of these values (rendered as escaped regex)
            Regex(...)   -> raw regex pattern, passed through unescaped
        At least one non-empty filter group is required.
    bbox : tuple[float, float, float, float] | None
        Bounding box as (south, west, north, east). Mutually exclusive
        with area_name; exactly one must be set.
    area_name : str | None
        Named area to search within (matched against the OSM "name"
        tag on a relation). Mutually exclusive with bbox.
    timeout : int
        Server-side timeout in seconds. Default 180.
    out_mode : str
        Output verbosity: "geom" (full geometry) or "center"
        (centroid only for ways/relations). "meta" is always included
        so we get version and timestamp on every element.
    """

    element_types: list[str]
    tag_filters: list[dict[str, str | list[str] | Regex | None]]
    bbox: tuple[float, float, float, float] | None = None
    area_name: str | None = None
    timeout: int = 180
    out_mode: str = "geom"

    def __post_init__(self) -> None:
        # element_types
        if not self.element_types:
            raise ValueError("element_types must be non-empty")
        invalid = set(self.element_types) - VALID_ELEMENT_TYPES
        if invalid:
            raise ValueError(
                f"Invalid element_types: {sorted(invalid)}. "
                f"Must be a subset of {sorted(VALID_ELEMENT_TYPES)}."
            )

        # tag_filters
        if not self.tag_filters:
            raise ValueError(
                "tag_filters must be non-empty. A query with no tag filter "
                "would return every element in the area."
            )
        for i, group in enumerate(self.tag_filters):
            if not group:
                raise ValueError(
                    f"tag_filters[{i}] is empty. Each filter group must have at least one key."
                )

        # spatial extent: exactly one of bbox / area_name
        if self.bbox is None and self.area_name is None:
            raise ValueError("Exactly one of bbox or area_name must be set.")
        if self.bbox is not None and self.area_name is not None:
            raise ValueError("bbox and area_name are mutually exclusive.")

        if self.bbox is not None:
            s, w, n, e = self.bbox
            if not (-90 <= s <= 90 and -90 <= n <= 90):
                raise ValueError(f"Latitude out of range in bbox: {self.bbox}")
            if not (-180 <= w <= 180 and -180 <= e <= 180):
                raise ValueError(f"Longitude out of range in bbox: {self.bbox}")
            if s >= n:
                raise ValueError(f"bbox south ({s}) must be less than north ({n})")

        # out_mode
        if self.out_mode not in VALID_OUT_MODES:
            raise ValueError(
                f"out_mode must be one of {sorted(VALID_OUT_MODES)}, got {self.out_mode!r}"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def to_ql(self, date_filter: str | None = None) -> str:
        """
        Render this query as an Overpass QL string.

        Parameters
        ----------
        date_filter : str | None
            ISO-8601 timestamp (e.g. "2026-04-01T00:00:00Z"). When set,
            a (newer:"<timestamp>") filter is appended to every selector,
            limiting results to elements edited after that time.

        Returns
        -------
        str
            A complete Overpass QL query, ready to POST to the API.
        """
        lines = [f"[out:json][timeout:{self.timeout}];"]

        if self.area_name is not None:
            lines.append(f'area["name"={_quote(self.area_name)}]->.searchArea;')

        # Build the union block of selectors: one per (element_type, filter_group).
        lines.append("(")
        for element_type in self.element_types:
            for group in self.tag_filters:
                lines.append(f"  {self._render_selector(element_type, group, date_filter)};")
        lines.append(");")

        lines.append(f"out {self.out_mode} meta;")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _render_selector(
        self,
        element_type: str,
        filter_group: dict[str, str | list[str] | None],
        date_filter: str | None,
    ) -> str:
        """Render one selector: element type + tag filters + spatial + date."""
        parts = [element_type]

        for key, value in filter_group.items():
            parts.append(_render_tag_filter(key, value))

        if date_filter is not None:
            parts.append(f"(newer:{_quote(date_filter)})")

        if self.bbox is not None:
            s, w, n, e = self.bbox
            parts.append(f"({s},{w},{n},{e})")
        else:
            parts.append("(area.searchArea)")

        return "".join(parts)


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _quote(value: str) -> str:
    """Quote a string for use inside Overpass QL, escaping embedded quotes."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _render_tag_filter(key: str, value: str | list[str] | Regex | None) -> str:
    """
    Render one entry from a filter group as an Overpass tag filter.

    Cases:
        value is None      -> ["key"]              (key exists)
        value is a str     -> ["key"="value"]      (exact match)
        value is a list    -> ["key"~"^(a|b)$"]    (any of these values, escaped)
        value is Regex(p)  -> ["key"~"<p>"]        (raw regex, passed through)
    """
    quoted_key = _quote(key)

    if value is None:
        return f"[{quoted_key}]"

    if isinstance(value, Regex):
        modifier = ",i" if value.case_insensitive else ""
        return f"[{quoted_key}~{_quote(value.pattern)}{modifier}]"

    if isinstance(value, list):
        if not value:
            raise ValueError(f"Empty value list for tag key {key!r}")
        # Escape regex metacharacters in each value, then join with |.
        escaped = [_regex_escape(v) for v in value]
        pattern = f"^({'|'.join(escaped)})$"
        return f"[{quoted_key}~{_quote(pattern)}]"

    return f"[{quoted_key}={_quote(value)}]"


def _regex_escape(value: str) -> str:
    """Escape regex metacharacters so a tag value matches literally."""
    # Characters that have special meaning in Overpass's regex flavor (POSIX ERE).
    specials = r".^$*+?()[]{}|\\"
    out = []
    for ch in value:
        if ch in specials:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)
