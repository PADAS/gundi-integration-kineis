from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List, Optional, Literal, Tuple

from pydantic import BaseModel, Field, root_validator, validator


# ============================================================================
# Utilities
# ============================================================================

def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_utc_z(dt: Optional[datetime]) -> Optional[str]:
    dt = ensure_utc(dt)
    if dt is None:
        return None
    return dt.isoformat().replace("+00:00", "Z")


def is_zero_zero(lat: float, lon: float) -> bool:
    return abs(lat) < 1e-9 and abs(lon) < 1e-9


def classify_doppler_confidence(
    doppler_class: Optional[str],
    error_m: Optional[float],
    nb_msg: Optional[int],
) -> str:
    doppler_class = (doppler_class or "").upper()

    if doppler_class in {"3", "2"}:
        return "high"
    if doppler_class == "1":
        return "medium"
    if doppler_class == "0":
        return "medium" if error_m is not None and error_m <= 1500 else "low"
    if doppler_class in {"A", "B"}:
        if error_m is not None and error_m <= 1000 and (nb_msg or 0) >= 3:
            return "medium"
        return "low"

    if error_m is not None:
        if error_m <= 500:
            return "high"
        if error_m <= 1500:
            return "medium"

    return "low"


# ============================================================================
# Raw source models
# ============================================================================

class KineisMetadata(BaseModel):
    sat: Optional[str] = None
    mod: Optional[str] = None
    level: Optional[float] = None
    snr: Optional[float] = None
    freq: Optional[float] = None


class KineisRawMessage(BaseModel):
    deviceMsgUid: Optional[int] = None
    providerMsgId: Optional[int] = None
    msgType: Optional[str] = None

    deviceUid: Optional[int] = None
    deviceRef: Optional[str] = None
    modemRef: Optional[str] = None

    msgDatetime: Optional[datetime] = None
    acqDatetime: Optional[datetime] = None

    kineisMetadata: Optional[KineisMetadata] = None

    rawData: Optional[str] = None
    bitLength: Optional[int] = None

    dopplerLocId: Optional[int] = None
    dopplerRevision: Optional[int] = None
    dopplerDatetime: Optional[datetime] = None
    dopplerAcqDatetime: Optional[datetime] = None
    dopplerLocLon: Optional[float] = None
    dopplerLocLat: Optional[float] = None
    dopplerLocAlt: Optional[float] = None
    dopplerLocErrorRadius: Optional[float] = None
    dopplerDeviceFrequency: Optional[float] = None
    dopplerLocClass: Optional[str] = None
    dopplerNbMsg: Optional[int] = None

    @validator("msgDatetime", "acqDatetime", "dopplerDatetime", "dopplerAcqDatetime", pre=False, always=False)
    def normalize_datetimes(cls, v: Optional[datetime]) -> Optional[datetime]:
        return ensure_utc(v)

    @validator("dopplerLocLat")
    def validate_doppler_lat(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90.0 <= v <= 90.0):
            raise ValueError("dopplerLocLat must be between -90 and 90")
        return v

    @validator("dopplerLocLon")
    def validate_doppler_lon(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180.0 <= v <= 180.0):
            raise ValueError("dopplerLocLon must be between -180 and 180")
        return v

    @property
    def external_message_id(self) -> Optional[str]:
        if self.providerMsgId is not None:
            return str(self.providerMsgId)
        if self.deviceMsgUid is not None:
            return str(self.deviceMsgUid)
        if self.deviceUid is not None and self.msgDatetime is not None:
            raw = self.rawData or ""
            h = sha256(f"{self.deviceUid}|{self.msgDatetime.isoformat()}|{raw}".encode("utf-8")).hexdigest()[:16]
            return f"{self.deviceUid}-{h}"
        return None

    @property
    def idempotency_key(self) -> str:
        return f"cls_kineis:{self.external_message_id or 'unknown'}"


# ============================================================================
# Decoded / normalized models
# ============================================================================

class DecodedPayloadLocation(BaseModel):
    latitude: float
    longitude: float
    fix_timestamp: Optional[datetime] = None
    accuracy_m: Optional[float] = None
    altitude_m: Optional[float] = None
    quality_class: Optional[str] = None
    source: str = "tag_gps_payload"

    @validator("latitude")
    def validate_latitude(cls, v: float) -> float:
        if not (-90.0 <= v <= 90.0):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @validator("longitude")
    def validate_longitude(cls, v: float) -> float:
        if not (-180.0 <= v <= 180.0):
            raise ValueError("longitude must be between -180 and 180")
        return v

    @validator("fix_timestamp", pre=False, always=False)
    def normalize_fix_timestamp(cls, v: Optional[datetime]) -> Optional[datetime]:
        return ensure_utc(v)


class PayloadDecodeResult(BaseModel):
    decoded_payload: Optional[Dict[str, Any]] = None
    payload_location: Optional[DecodedPayloadLocation] = None
    decoder_name: Optional[str] = None
    decoder_version: Optional[str] = None


class NormalizedKineisMessage(BaseModel):
    idempotency_key: str
    external_message_id: Optional[str] = None
    device_id: Optional[str] = None
    device_ref: Optional[str] = None
    modem_ref: Optional[str] = None
    platform: Literal["cls_kineis"] = "cls_kineis"

    message_timestamp: Optional[datetime] = None
    message_received_timestamp: Optional[datetime] = None

    message_kind: Literal["telemetry_with_location", "telemetry_without_location"]
    location_status: Literal["located", "unlocated", "invalid"]
    location_method: Optional[Literal["gps_payload", "doppler"]] = None

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_m: Optional[float] = None
    location_timestamp: Optional[datetime] = None
    location_error_m: Optional[float] = None
    location_quality_class: Optional[str] = None
    location_confidence: Optional[Literal["high", "medium", "low"]] = None
    secondary_location_method: Optional[Literal["doppler"]] = None

    best_latitude: Optional[float] = None
    best_longitude: Optional[float] = None
    best_altitude_m: Optional[float] = None
    best_location_timestamp: Optional[datetime] = None
    best_location_method: Optional[Literal["gps_payload", "doppler"]] = None
    best_location_error_m: Optional[float] = None
    best_location_quality_class: Optional[str] = None

    location_source: Optional[str] = None
    location_validation_reason: Optional[str] = None

    satellite: Optional[str] = None
    signal_level_dbm: Optional[float] = None
    snr: Optional[float] = None
    uplink_frequency_hz: Optional[float] = None

    payload_raw_hex: Optional[str] = None
    payload_bit_length: Optional[int] = None
    payload_decoded: Optional[Dict[str, Any]] = None
    decoder_name: Optional[str] = None
    decoder_version: Optional[str] = None

    source_details: Dict[str, Any] = Field(default_factory=dict)
    raw_message: Dict[str, Any] = Field(default_factory=dict)

    @validator(
        "message_timestamp",
        "message_received_timestamp",
        "location_timestamp",
        "best_location_timestamp",
        pre=False,
        always=False,
    )
    def normalize_datetimes(cls, v: Optional[datetime]) -> Optional[datetime]:
        return ensure_utc(v)

    @validator("latitude", "best_latitude")
    def validate_latitudes(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90.0 <= v <= 90.0):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @validator("longitude", "best_longitude")
    def validate_longitudes(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180.0 <= v <= 180.0):
            raise ValueError("longitude must be between -180 and 180")
        return v

    @root_validator
    def validate_location_consistency(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        location_status = values.get("location_status")
        best_latitude = values.get("best_latitude")
        best_longitude = values.get("best_longitude")
        best_location_method = values.get("best_location_method")

        if location_status == "located":
            if best_latitude is None or best_longitude is None or best_location_method is None:
                raise ValueError("located messages must have best location fields populated")

        return values


# ============================================================================
# EarthRanger-style models
# ============================================================================

class EarthRangerGeometry(BaseModel):
    type: Literal["Point"] = "Point"
    coordinates: List[float]

    @validator("coordinates")
    def validate_coordinates(cls, v: List[float]) -> List[float]:
        if len(v) != 2:
            raise ValueError("coordinates must be [lon, lat]")
        lon, lat = v
        if not (-180.0 <= lon <= 180.0):
            raise ValueError("longitude out of range")
        if not (-90.0 <= lat <= 90.0):
            raise ValueError("latitude out of range")
        return v


class EarthRangerObservation(BaseModel):
    source: str
    subject_id: str
    recorded_at: datetime
    geometry: EarthRangerGeometry
    properties: Dict[str, Any]

    @validator("recorded_at", pre=False, always=True)
    def normalize_recorded_at(cls, v: datetime) -> datetime:
        return ensure_utc(v)


# ============================================================================
# Decoder hook
# ============================================================================

class KineisPayloadDecoder:
    """
    Replace or subclass this with a real decoder for your CLS templates.
    """

    def decode(self, message: KineisRawMessage) -> PayloadDecodeResult:
        return PayloadDecodeResult(
            decoded_payload=None,
            payload_location=None,
            decoder_name=None,
            decoder_version=None,
        )


# ============================================================================
# Normalizer / Classifier
# ============================================================================

class KineisMessageNormalizer:
    def __init__(self, decoder: Optional[KineisPayloadDecoder] = None):
        self.decoder = decoder or KineisPayloadDecoder()

    def normalize(self, message: KineisRawMessage) -> NormalizedKineisMessage:
        decode_result = self.decoder.decode(message)

        has_doppler = (
            message.dopplerLocLat is not None and
            message.dopplerLocLon is not None
        )

        doppler_valid = False
        doppler_invalid_reason = None
        doppler_confidence = None

        if has_doppler:
            if is_zero_zero(message.dopplerLocLat, message.dopplerLocLon):
                doppler_invalid_reason = "doppler_coordinates_zero_zero"
            else:
                doppler_valid = True
                doppler_confidence = classify_doppler_confidence(
                    doppler_class=message.dopplerLocClass,
                    error_m=message.dopplerLocErrorRadius,
                    nb_msg=message.dopplerNbMsg,
                )

        payload_location = decode_result.payload_location
        payload_gps_valid = False
        payload_gps_invalid_reason = None

        if payload_location is not None:
            if is_zero_zero(payload_location.latitude, payload_location.longitude):
                payload_gps_invalid_reason = "payload_gps_coordinates_zero_zero"
            else:
                payload_gps_valid = True

        if payload_gps_valid:
            location_status = "located"
            location_method = "gps_payload"
            latitude = payload_location.latitude
            longitude = payload_location.longitude
            altitude_m = payload_location.altitude_m
            location_timestamp = payload_location.fix_timestamp or message.msgDatetime
            location_error_m = payload_location.accuracy_m
            location_quality_class = payload_location.quality_class
            location_confidence = "high" if (payload_location.accuracy_m or 999999.0) <= 100.0 else "medium"
            secondary_location_method = "doppler" if doppler_valid else None
            location_source = payload_location.source
            location_validation_reason = None

        elif doppler_valid:
            location_status = "located"
            location_method = "doppler"
            latitude = message.dopplerLocLat
            longitude = message.dopplerLocLon
            altitude_m = message.dopplerLocAlt
            location_timestamp = message.dopplerDatetime or message.msgDatetime
            location_error_m = message.dopplerLocErrorRadius
            location_quality_class = message.dopplerLocClass
            location_confidence = doppler_confidence
            secondary_location_method = None
            location_source = "kineis_doppler_solution"
            location_validation_reason = None

        elif payload_location is not None or has_doppler:
            location_status = "invalid"
            location_method = None
            latitude = None
            longitude = None
            altitude_m = None
            location_timestamp = None
            location_error_m = None
            location_quality_class = None
            location_confidence = None
            secondary_location_method = None
            location_source = None
            location_validation_reason = payload_gps_invalid_reason or doppler_invalid_reason

        else:
            location_status = "unlocated"
            location_method = None
            latitude = None
            longitude = None
            altitude_m = None
            location_timestamp = None
            location_error_m = None
            location_quality_class = None
            location_confidence = None
            secondary_location_method = None
            location_source = None
            location_validation_reason = "no_doppler_solution_and_no_payload_gps"

        message_kind = "telemetry_with_location" if location_status == "located" else "telemetry_without_location"

        source_details = {
            "deviceMsgUid": message.deviceMsgUid,
            "providerMsgId": message.providerMsgId,
            "msgType": message.msgType,
            "dopplerLocId": message.dopplerLocId,
            "dopplerRevision": message.dopplerRevision,
            "dopplerAcqDatetime": to_utc_z(message.dopplerAcqDatetime),
            "dopplerNbMsg": message.dopplerNbMsg,
            "dopplerDeviceFrequency": message.dopplerDeviceFrequency,
            "kineisMetadata": message.kineisMetadata.dict() if message.kineisMetadata else None,
        }

        return NormalizedKineisMessage(
            idempotency_key=message.idempotency_key,
            external_message_id=message.external_message_id,
            device_id=str(message.deviceUid) if message.deviceUid is not None else None,
            device_ref=message.deviceRef,
            modem_ref=message.modemRef,
            platform="cls_kineis",

            message_timestamp=message.msgDatetime,
            message_received_timestamp=message.acqDatetime,

            message_kind=message_kind,
            location_status=location_status,
            location_method=location_method,

            latitude=latitude,
            longitude=longitude,
            altitude_m=altitude_m,
            location_timestamp=location_timestamp,
            location_error_m=location_error_m,
            location_quality_class=location_quality_class,
            location_confidence=location_confidence,
            secondary_location_method=secondary_location_method,

            best_latitude=latitude,
            best_longitude=longitude,
            best_altitude_m=altitude_m,
            best_location_timestamp=location_timestamp,
            best_location_method=location_method,
            best_location_error_m=location_error_m,
            best_location_quality_class=location_quality_class,

            location_source=location_source,
            location_validation_reason=location_validation_reason,

            satellite=message.kineisMetadata.sat if message.kineisMetadata else None,
            signal_level_dbm=message.kineisMetadata.level if message.kineisMetadata else None,
            snr=message.kineisMetadata.snr if message.kineisMetadata else None,
            uplink_frequency_hz=message.kineisMetadata.freq if message.kineisMetadata else None,

            payload_raw_hex=message.rawData,
            payload_bit_length=message.bitLength,
            payload_decoded=decode_result.decoded_payload,
            decoder_name=decode_result.decoder_name,
            decoder_version=decode_result.decoder_version,

            source_details=source_details,
            raw_message=message.dict(),
        )


# ============================================================================
# Mapper
# ============================================================================

class EarthRangerMapper:
    def build_observation(
        self,
        normalized: NormalizedKineisMessage,
        subject_id: Optional[str] = None,
        source_name: str = "cls_kineis",
        include_raw: bool = False,
    ) -> Optional[EarthRangerObservation]:
        if normalized.location_status != "located":
            return None

        subject = subject_id or normalized.device_ref or normalized.device_id
        if not subject:
            raise ValueError("subject_id, device_ref, or device_id is required")

        recorded_at = normalized.best_location_timestamp or normalized.message_timestamp
        if recorded_at is None:
            raise ValueError("located observation requires recorded_at")

        properties: Dict[str, Any] = {
            "idempotency_key": normalized.idempotency_key,
            "external_message_id": normalized.external_message_id,
            "device_id": normalized.device_id,
            "device_ref": normalized.device_ref,
            "modem_ref": normalized.modem_ref,

            "platform": normalized.platform,
            "message_kind": normalized.message_kind,
            "message_timestamp": to_utc_z(normalized.message_timestamp),
            "message_received_timestamp": to_utc_z(normalized.message_received_timestamp),

            "location_status": normalized.location_status,
            "location_method": normalized.location_method,
            "location_source": normalized.location_source,
            "location_error_m": normalized.location_error_m,
            "location_quality_class": normalized.location_quality_class,
            "location_confidence": normalized.location_confidence,
            "secondary_location_method": normalized.secondary_location_method,

            "satellite": normalized.satellite,
            "signal_level_dbm": normalized.signal_level_dbm,
            "snr": normalized.snr,
            "uplink_frequency_hz": normalized.uplink_frequency_hz,

            "payload_bit_length": normalized.payload_bit_length,
            "decoder_name": normalized.decoder_name,
            "decoder_version": normalized.decoder_version,

            "source_details": normalized.source_details,
        }

        if normalized.payload_decoded is not None:
            properties["payload_decoded"] = normalized.payload_decoded

        if include_raw:
            properties["payload_raw_hex"] = normalized.payload_raw_hex
            properties["raw_message"] = normalized.raw_message

        return EarthRangerObservation(
            source=source_name,
            subject_id=subject,
            recorded_at=recorded_at,
            geometry=EarthRangerGeometry(
                coordinates=[normalized.best_longitude, normalized.best_latitude]
            ),
            properties=properties,
        )

    def build_message_audit_record(
        self,
        normalized: NormalizedKineisMessage,
    ) -> Dict[str, Any]:
        return {
            "idempotency_key": normalized.idempotency_key,
            "external_message_id": normalized.external_message_id,
            "platform": normalized.platform,
            "device_id": normalized.device_id,
            "device_ref": normalized.device_ref,
            "message_timestamp": to_utc_z(normalized.message_timestamp),
            "message_received_timestamp": to_utc_z(normalized.message_received_timestamp),
            "message_kind": normalized.message_kind,
            "location_status": normalized.location_status,
            "location_method": normalized.location_method,
            "location_validation_reason": normalized.location_validation_reason,
            "source_details": normalized.source_details,
            "payload_decoded": normalized.payload_decoded,
            "payload_raw_hex": normalized.payload_raw_hex,
        }
