"""
Configuration adapters for mapping native specifications from DRM to DRMAA API
"""

from typing import (Dict, List, ClassVar, Bool, Union, Any, Optional,
                    TYPE_CHECKING)

from dataclasses import dataclass, asdict, fields, InitVar
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from drmaa import JobTemplate

# DRMAA specific fields, anything else should be put into native spec
DRMAA_FIELDS = [
    "email", "deadlineTime", "errorPath", "hardRunDurationLimit",
    "hardWallclockTimeLimit", "inputPath", "outputPath", "jobCategory",
    "jobName", "outputPath", "workingDirectory", "transferFiles",
    "remoteCommand", "args", "jobName", "jobCategory", "blockEmail"
]


@dataclass
class DRMAACompatible(ABC):
    '''
    Abstract dataclass for mapping DRM specific configuration to a
    DRMAA compatible specification

    Properties:
        _mapped_fields: List of DRM specific keys to re-map onto
            the DRMAA specification if used. Preferably users will
            use the DRMAA variant of these specifications rather than
            the corresponding native specification
    '''

    _mapped_fields: ClassVar[Dict[str, Any]]

    def __str__(self):
        '''
        Display formatted configuration for executor
        '''
        attrs = asdict(self)
        drmaa_fields = "\n".join([
            f"{field}:\t{attrs.get(field)}" for field in DRMAA_FIELDS
            if attrs.get(field) is not None
        ])

        drm_fields = "\n".join([
            f"{field}:\t{attrs.get(field)}" for field in self._native_fields()
            if attrs.get(field) is not None
        ])

        return ("DRMAA Config:\n" + drmaa_fields + "\nNative Specification\n" +
                drm_fields)

    def get_drmaa_config(self, jt: JobTemplate) -> JobTemplate:
        '''
        Apply settings onto DRMAA JobTemplate
        '''
        for field in DRMAA_FIELDS:
            value = getattr(self, field)
            if value:
                setattr(jt, field, value)

        jt.nativeSpecification = self.drm2drmaa()
        return jt

    @abstractmethod
    def drm2drmaa(self) -> str:
        '''
        Build native specification from DRM-specific fields
        '''

    def _map_fields(self, drm_kwargs: Dict[str, Any]):
        '''
        Transform fields in `_mapped_fields` to
        DRMAA-compliant specification. Adds
        DRM-specific attributes to `self`

        Arguments:
            drm_kwargs: DRM-specific key-value pairs
        '''
        for drm_name, value in drm_kwargs.item():
            try:
                drmaa_name = self._mapped_fields[drm_name]
            except KeyError:
                raise AttributeError(
                    "Malformed adapter class! Cannot map field"
                    f"{drm_name} to a DRMAA-compliant field")

            setattr(self, drmaa_name, value)

    def __post_init__(self, **kwargs):
        self._map_fields(**kwargs)

    def _native_fields(self):
        return [
            f for f in asdict(self).keys()
            if (f not in self._mapped_fields.keys()) and (
                f not in DRMAA_FIELDS)
        ]


@dataclass
class SlurmConfig(DRMConfigAdapter):
    '''
    Transform SLURM resource specification into DRMAA-compliant inputs

    References:
        See https://github.com/natefoo/slurm-drmaa for native specification
        details
    '''

    _mapped_fields: ClassVar[Dict[str, Any]] = {
        "error": "errorPath",
        "output": "outputPath",
        "job_name": "jobName",
        "time": "hardWallclockTimeLimit"
    }

    job_name: InitVar[str]
    time: InitVar[str]
    error: Optional[InitVar[str]] = None
    output: Optional[InitVar[str]] = None

    account: Optional[str] = None
    acctg_freq: Optional[str] = None
    comment: Optional[str] = None
    constraint: Optional[List] = None
    cpus_per_task: Optional[int] = None
    contiguous: Optional[Bool] = None
    dependency: Optional[List] = None
    exclusive: Optional[Bool] = None
    gres: Optional[Union[List[str], str]] = None
    no_kill: Optional[Bool] = None
    licenses: Optional[List[str]] = None
    clusters: Optional[Union[List[str], str]] = None
    mail_type: Optional[str] = None
    mem: Optional[int] = None
    mincpus: Optional[int] = None
    nodes: Optional[int] = None
    ntasks: Optional[int] = None
    no_requeue: Optional[Bool] = None
    ntasks_per_node: Optional[int] = None
    partition: Optional[int] = None
    qos: Optional[str] = None
    requeue: Optional[Bool] = None
    reservation: Optional[str] = None
    share: Optional[Bool] = None
    tmp: Optional[str] = None
    nodelist: Optional[Union[List[str], str]] = None
    exclude: Optional[Union[List[str], str]] = None

    def __post_init__(self, job_name, time, error, output):
        '''
        Transform Union[List[str]] --> comma-delimited str
        '''

        super().__post_init__(job_name=job_name,
                              time=time,
                              error=error,
                              output=output)

        for field in fields(self):
            value = getattr(self, field.name)
            if field.type == Union[List[str], str] and isinstance(value, list):
                setattr(self, field.name, ",".join(value))

    @abstractmethod
    def drm2drmaa(self) -> str:
        return self._transform_attrs()

    def _transform_attrs(self) -> str:
        '''
        Remap named attributes to "-" form, excludes renaming
        DRMAA-compliant fields (set in __post_init__()) then join
        attributes into a nativeSpecification string
        '''

        out = []
        for field in self._native_fields():

            value = getattr(self, field)
            if value is None:
                continue

            field_fmtd = field.replace("_", "-")
            if isinstance(value, bool):
                out.append(f"--{field_fmtd}")
            else:
                out.append(f"--{field_fmtd}={value}")
        return " ".join(out)
