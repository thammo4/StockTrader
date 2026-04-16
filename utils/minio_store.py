#
# FILE: `StockTrader/utils/minio_store.py`
#

import io
import os
import json
from pathlib import PurePosixPath

import pandas as pd
from minio import Minio
from minio.error import S3Error


def _build_client(endpoint: str = None, access_key: str = None, secret_key: str = None, secure: bool = False) -> Minio:
    endpoint = endpoint or os.environ.get("MINIO_ENDPOINT")
    access_key = access_key or os.environ.get("MINIO_ROOT_USER")
    secret_key = secret_key or os.environ.get("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError("Bad minio creds")

    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


class MinioStore:
    def __init__(self, endpoint: str = None, access_key: str = None, secret_key: str = None, secure: bool = False):
        self._client = _build_client(endpoint, access_key, secret_key, secure)

    @property
    def client(self) -> Minio:
        return self._client

    @staticmethod
    def _apply_path_cols(df: pd.DataFrame, obj_name: str, prefix: str, fname_col):
        """Extract metadata cols from obj key path segments"""

        path_relative = obj_name[len(prefix) :]
        path_parts = PurePosixPath(path_relative).parts
        path_parts = list(path_parts[:-1]) + [PurePosixPath(path_parts[-1]).stem]

        if isinstance(fname_col, str):
            df[fname_col] = path_parts[-1]
        else:
            if len(fname_col) > len(path_parts):
                raise ValueError(
                    f"fname_col has {len(fname_col)} names" f"'{path_relative} has {len(path_parts)} sgmts"
                )
            for name, val in zip(reversed(fname_col), reversed(path_parts)):
                df[name] = val

    def _ensure_bucket(self, bucket: str):
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)

    def _read_bytes(self, bucket: str, obj_name: str) -> bytes:
        r = None
        try:
            r = self._client.get_object(bucket, obj_name)
            return r.read()
        except S3Error as e:
            raise FileNotFoundError(f"s3://{bucket}/{obj_name}") from e
        finally:
            if r is not None:
                r.close()
                r.drain_conn()

    def _read_single(self, bucket: str, obj_name: str) -> pd.DataFrame:
        return pd.read_parquet(io.BytesIO(self._read_bytes(bucket, obj_name)))

    def read_prefix(self, bucket: str, prefix: str = "", fmt: str = "parquet", fname_col: str | list[str] = None):
        ext = f".{fmt}"
        objs = [
            o for o in self._client.list_objects(bucket, prefix=prefix, recursive=True) if o.object_name.endswith(ext)
        ]
        if not objs:
            raise FileNotFoundError(f"No {fmt} files at s3://{bucket}/{prefix}")

        readers = {
            "csv": pd.read_csv,
            "json": lambda b: pd.DataFrame([json.loads(b.read())]),
            "parquet": pd.read_parquet,
        }
        if fmt not in readers:
            raise ValueError(f"Nope: {fmt}")

        r = readers[fmt]
        frames = []
        for o in objs:
            df = r(io.BytesIO(self._read_bytes(bucket, o.object_name)))
            # if fname_col is not None:
            #     df[fname_col] = PurePosixPath(o.object_name).stem
            # frames.append(df)
            if fname_col is not None:
                self._apply_path_cols(df, o.object_name, prefix, fname_col)
            frames.append(df)

        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    def read_parquet(
        self, bucket: str, obj_name: str = None, prefix: str = None, fname_col: str | list[str] = None
    ) -> pd.DataFrame:
        if obj_name and prefix:
            raise ValueError("Provide obj_name or prefix not both [minio_store]")
        if not obj_name and not prefix:
            raise ValueError("Provide obj_name (1-file) or prefix (n-files)")

        if obj_name:
            df = self._read_single(bucket, obj_name)
            if fname_col is not None:
                self._apply_path_cols(df, obj_name, "", fname_col)
            return df
            # if fname_col is not None:
            #     df[fname_col] = PurePosixPath(obj_name).stem
            # return df

        return self.read_prefix(bucket, prefix, fmt="parquet", fname_col=fname_col)

    def write_parquet(
        self, bucket: str, obj_name: str, df: pd.DataFrame, compress: str = "zstd", ensure_bucket: bool = False
    ) -> str:
        if ensure_bucket:
            self._ensure_bucket(bucket)

        b = io.BytesIO()
        df.to_parquet(b, index=False, engine="pyarrow", compression=compress)
        b.seek(0)

        self._client.put_object(
            bucket_name=bucket,
            object_name=obj_name,
            data=b,
            length=b.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        return f"s3://{bucket}/{obj_name}"

    def write_json(self, bucket: str, obj_name: str, data: dict, ensure_bucket: bool = False) -> str:
        if ensure_bucket:
            self._ensure_bucket(bucket)

        raw = json.dumps(data, indent=2, default=str).encode("utf-8")
        b = io.BytesIO(raw)

        self._client.put_object(
            bucket_name=bucket, object_name=obj_name, data=b, length=len(raw), content_type="application/json"
        )

        return f"s3://{bucket}/{obj_name}"
