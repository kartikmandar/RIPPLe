"""
Utility for saving extracted cutouts to local disk

This module provides functionality to save multi-band cutouts in various formats
suitable for machine learning training data preparation.
"""

import numpy as np
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from astropy.io import fits
from datetime import datetime

class CutoutSaver:
    """
    Save multi-band cutouts with metadata for ML training
    """

    def __init__(self, output_dir: str = "./results/cutouts"):
        """
        Initialize the cutout saver.

        Args:
            output_dir: Base directory for saving cutouts
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for organization
        self.bands_dir = self.output_dir / "bands"
        self.rgb_dir = self.output_dir / "rgb_composites"
        self.metadata_dir = self.output_dir / "metadata"

        for subdir in [self.bands_dir, self.rgb_dir, self.metadata_dir]:
            subdir.mkdir(exist_ok=True)

        self.logger = logging.getLogger(__name__)

    def save_multi_band_cutouts(self,
                            cutouts: Dict[str, Any],
                            ra: float,
                            dec: float,
                            label: str = None,
                            additional_metadata: Dict = None) -> Dict[str, str]:
        """
        Save multi-band cutouts with metadata.

        Args:
            cutouts: Dictionary with band names as keys and cutout data as values
            ra, dec: Sky coordinates
            label: Optional label for the cutout (e.g., lens candidate ID)
            additional_metadata: Additional metadata to save

        Returns:
            Dictionary mapping band/rgb to saved file paths
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        coord_str = f"{ra:.3f}_{dec:.3f}"
        label_str = f"_{label}" if label else ""
        base_filename = f"cutout_{timestamp}_{coord_str}{label_str}"

        saved_files = {}
        metadata = {
            "ra": ra,
            "dec": dec,
            "coordinates": {"ra": ra, "dec": dec},
            "label": label,
            "timestamp": timestamp,
            "bands_available": list(cutouts.keys())
        }

        # Save individual bands
        for band, cutout in cutouts.items():
            if cutout is not None:
                try:
                    # Extract image data from different cutout types
                    image_data = self._extract_image_data(cutout, band)

                    if image_data is not None:
                        # Save as FITS file with metadata
                        band_filename = f"{base_filename}_{band}.fits"
                        band_path = self.bands_dir / band_filename

                        # Create primary HDU with the image
                        hdu = fits.PrimaryHDU(image_data)

                        # Add header metadata
                        hdu.header['RA'] = (ra, 'Right ascension in degrees')
                        hdu.header['DEC'] = (dec, 'Declination in degrees')
                        hdu.header['BAND'] = (band, 'Filter band')
                        hdu.header['LABEL'] = (label or '', 'Cutout label')

                        # Save FITS file
                        hdu.writeto(band_path, overwrite=True)
                        saved_files[f"{band}_fits"] = str(band_path)

                        # Also save as numpy array
                        npy_filename = f"{base_filename}_{band}.npy"
                        npy_path = self.bands_dir / npy_filename
                        np.save(npy_path, image_data)
                        saved_files[f"{band}_npy"] = str(npy_path)

                        self.logger.info(f"✓ Saved {band} band: {band_filename}")

                        # Add band-specific metadata
                        metadata[f"{band}_info"] = {
                            "shape": image_data.shape,
                            "dtype": str(image_data.dtype),
                            "min": float(np.min(image_data)),
                            "max": float(np.max(image_data)),
                            "mean": float(np.mean(image_data)),
                            "std": float(np.std(image_data))
                        }

                except Exception as e:
                    self.logger.error(f"Failed to save {band} band: {e}")

        # Create and save RGB composite if we have g, r, i bands
        rgb_bands = ['g', 'r', 'i']
        if all(band in cutouts for band in rgb_bands):
            try:
                rgb_image = self._create_rgb_composite(cutouts)
                if rgb_image is not None:
                    rgb_filename = f"{base_filename}_rgb.npy"
                    rgb_path = self.rgb_dir / rgb_filename
                    np.save(rgb_path, rgb_image)
                    saved_files["rgb_npy"] = str(rgb_path)

                    self.logger.info(f"✓ Saved RGB composite: {rgb_filename}")

            except Exception as e:
                self.logger.error(f"Failed to create RGB composite: {e}")

        # Merge additional metadata
        if additional_metadata:
            metadata.update(additional_metadata)

        # Save metadata
        metadata_filename = f"{base_filename}_metadata.json"
        metadata_path = self.metadata_dir / metadata_filename

        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        saved_files["metadata"] = str(metadata_path)
        self.logger.info(f"✓ Saved metadata: {metadata_filename}")

        return saved_files

    def _extract_image_data(self, cutout: Any, band: str) -> Optional[np.ndarray]:
        """
        Extract image data from different cutout object types.

        Args:
            cutout: Cutout object (various types possible)
            band: Band name for logging

        Returns:
            Numpy array of image data or None if extraction fails
        """
        try:
            # Handle different cutout object types
            if hasattr(cutout, 'image'):
                # LSST Exposure object
                return cutout.image.array
            elif hasattr(cutout, 'array'):
                # Numpy-like array object
                return np.array(cutout.array)
            elif isinstance(cutout, np.ndarray):
                # Already a numpy array
                return cutout
            else:
                # Try to convert to numpy array
                return np.array(cutout)

        except Exception as e:
            self.logger.error(f"Failed to extract image data from {band} band cutout: {e}")
            return None

    def _create_rgb_composite(self, cutouts: Dict[str, Any]) -> Optional[np.ndarray]:
        """
        Create RGB composite from g, r, i bands.

        Args:
            cutouts: Dictionary with band cutouts

        Returns:
            RGB image as numpy array (H, W, 3) or None if failed
        """
        try:
            # Extract image data for each band
            band_data = {}
            for band in ['g', 'r', 'i']:
                image_data = self._extract_image_data(cutouts[band], band)
                if image_data is not None:
                    band_data[band] = image_data
                else:
                    return None

            # Normalize each band
            normalized_bands = {}
            for band, data in band_data.items():
                # Handle different data types and shapes
                arr = np.array(data, dtype=float)

                # Clip outliers (1st and 99th percentiles)
                arr = np.clip(arr,
                           np.percentile(arr, 1),
                           np.percentile(arr, 99))

                # Normalize to [0, 1]
                if arr.max() > arr.min():
                    arr = (arr - arr.min()) / (arr.max() - arr.min())
                else:
                    arr = np.zeros_like(arr)

                normalized_bands[band] = arr

            # Create RGB mapping: g->G, r->R, i->B (near-IR for blue channel)
            rgb = np.stack([
                normalized_bands['r'],  # Red channel
                normalized_bands['g'],  # Green channel
                normalized_bands['i']   # Blue channel (near-IR)
            ], axis=2)

            return rgb

        except Exception as e:
            self.logger.error(f"Failed to create RGB composite: {e}")
            return None

    def save_batch_cutouts(self,
                         batch_results: List[Dict],
                         coordinates: List[Tuple[float, float]],
                         labels: List[str] = None) -> Dict[str, List[str]]:
        """
        Save a batch of cutouts efficiently.

        Args:
            batch_results: List of cutout results from batch processing
            coordinates: List of (ra, dec) tuples
            labels: Optional list of labels

        Returns:
            Dictionary with lists of saved file paths
        """
        if labels is None:
            labels = [None] * len(coordinates)

        batch_saved_files = {
            "individual_bands": [],
            "rgb_composites": [],
            "metadata": []
        }

        for i, (result, (ra, dec), label) in enumerate(zip(batch_results, coordinates, labels)):
            if result.get("status") == "success" and result.get("cutout"):
                saved_files = self.save_multi_band_cutouts(
                    cutouts=result["cutout"],
                    ra=ra,
                    dec=dec,
                    label=label or f"batch_{i+1:03d}"
                )

                batch_saved_files["individual_bands"].extend([
                    v for k, v in saved_files.items() if k.endswith("_fits") or k.endswith("_npy")
                ])
                if "rgb_npy" in saved_files:
                    batch_saved_files["rgb_composites"].append(saved_files["rgb_npy"])
                if "metadata" in saved_files:
                    batch_saved_files["metadata"].append(saved_files["metadata"])
            else:
                self.logger.warning(f"Skipping cutout {i+1}: {result.get('error', 'Unknown error')}")

        summary = {
            "total_processed": len(batch_results),
            "successful_saves": len(batch_saved_files["metadata"]),
            "rgb_composites_saved": len(batch_saved_files["rgb_composites"]),
            "individual_bands_saved": len(batch_saved_files["individual_bands"])
        }

        self.logger.info(f"Batch save summary: {summary}")
        return batch_saved_files

    def get_output_directory(self) -> str:
        """Get the output directory path."""
        return str(self.output_dir)

    def get_saved_files_summary(self) -> Dict[str, int]:
        """
        Get a summary of saved files.

        Returns:
            Dictionary with counts of different file types
        """
        summary = {}

        for subdir_name, subdir_path in [
            ("bands", self.bands_dir),
            ("rgb_composites", self.rgb_dir),
            ("metadata", self.metadata_dir)
        ]:
            if subdir_path.exists():
                files = list(subdir_path.glob("*"))
                summary[subdir_name] = len(files)
            else:
                summary[subdir_name] = 0

        return summary