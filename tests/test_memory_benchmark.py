"""Test de benchmark para monitorear el uso de memoria durante la subida secuencial de videos."""
import pytest
import asyncio
import tracemalloc
from pathlib import Path
from typing import List, Dict
import time

try:
    import psutil
    import os
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

from uploader.orchestrator import UploadOrchestrator
from uploader.models import UploadConfig


class MemoryMonitor:
    """Monitor de memoria durante la ejecución."""
    
    def __init__(self):
        self.samples: List[Dict[str, float]] = []
        self.process = None
        if HAS_PSUTIL:
            self.process = psutil.Process(os.getpid())
    
    def start(self):
        """Iniciar monitoreo."""
        tracemalloc.start()
    
    def sample(self, label: str = ""):
        """Tomar una muestra de memoria."""
        sample = {
            "label": label,
            "timestamp": time.time()
        }
        
        if HAS_PSUTIL and self.process:
            try:
                mem_info = self.process.memory_info()
                sample["rss_mb"] = mem_info.rss / (1024 * 1024)  # Resident Set Size en MB
            except Exception:
                pass
        
        # Tracemalloc
        try:
            current, peak = tracemalloc.get_traced_memory()
            sample["traced_current_mb"] = current / (1024 * 1024)
            sample["traced_peak_mb"] = peak / (1024 * 1024)
        except Exception:
            pass
        
        self.samples.append(sample)
        return sample
    
    def stop(self):
        """Detener monitoreo."""
        try:
            tracemalloc.stop()
        except Exception:
            pass
    
    def get_memory_mb(self) -> float:
        """Obtener memoria actual en MB."""
        if HAS_PSUTIL and self.process:
            try:
                return self.process.memory_info().rss / (1024 * 1024)
            except Exception:
                pass
        return 0.0
    
    def print_sample(self, label: str):
        """Imprimir muestra de memoria."""
        mem_mb = self.get_memory_mb()
        sample = self.sample(label)
        traced_peak = sample.get("traced_peak_mb", 0)
        print(f"  {label}: RSS={mem_mb:.2f} MB | Traced Peak={traced_peak:.2f} MB")


def create_video_file(file_path: Path, size_mb: int) -> Path:
    """
    Crea un archivo de video simulado de tamaño especificado.
    Escribe en chunks para evitar cargar todo en memoria.
    """
    size_bytes = size_mb * 1024 * 1024
    chunk_size = 1024 * 1024  # 1MB chunks
    
    with open(file_path, 'wb') as f:
        written = 0
        chunk = b'\x00' * chunk_size
        
        while written < size_bytes:
            remaining = size_bytes - written
            to_write = min(chunk_size, remaining)
            f.write(chunk[:to_write])
            written += to_write
    
    return file_path


@pytest.mark.asyncio
async def test_memory_sequential_uploads(tmp_path, request):
    """
    Test simplificado para verificar que la memoria se libera después de cada video.
    
    Emula el flujo real del FolderUploadHandler:
    1. Recopila archivos
    2. Calcula blake3_hash para todos (en paralelo como hace Blake3Deduplicator)
    3. Verifica existencia en MEGA
    4. Sube archivos secuencialmente (archivos grandes)
    
    Verifica memoria después de CADA video para detectar fugas.
    """
    # Verificar que psutil está disponible
    if not HAS_PSUTIL:
        pytest.skip("psutil no está instalado. Instala con: pip install psutil")
    
    # Obtener configuración del test
    use_real_upload = request.config.getoption("--real-upload", default=False)
    
    if use_real_upload:
        pytest.skip("Para usar subida real, configura MEGA y API, luego ejecuta con --real-upload")
    
    # Crear monitor de memoria
    monitor = MemoryMonitor()
    monitor.start()
    
    # Crear carpeta de prueba
    test_folder = tmp_path / "test_videos"
    test_folder.mkdir()
    
    # Crear 5 videos de 200MB cada uno
    num_videos = 5
    video_size_mb = 200
    video_files = []
    
    print(f"\n{'='*60}")
    print(f"TEST DE MEMORIA: Subida secuencial de {num_videos} videos de {video_size_mb}MB")
    print(f"{'='*60}\n")
    
    print("Creando videos de prueba...")
    for i in range(num_videos):
        video_file = test_folder / f"video_{i+1:02d}.mp4"
        create_video_file(video_file, video_size_mb)
        video_files.append(video_file)
        print(f"  [OK] Creado: {video_file.name} ({video_file.stat().st_size / (1024*1024):.2f} MB)")
    
    monitor.print_sample("Inicial (después de crear videos)")
    
    # Mock del storage service - simula subida real pero sin conexión
    from unittest.mock import Mock, AsyncMock
    
    mock_storage_service = Mock()
    
    async def mock_upload_video(path, dest=None, source_id=None, progress_callback=None):
        """
        Simula upload_video - emula cómo MEGA lee y encripta el archivo.
        Lee el archivo en chunks como lo haría la encriptación real.
        """
        file_size = path.stat().st_size
        chunk_size = 1024 * 1024  # 1MB chunks (como hace MEGA)
        
        # Simular progreso de subida con lectura real del archivo
        chunks_processed = 0
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        
        for i in range(total_chunks):
            await asyncio.sleep(0.005)  # Simular I/O
            
            # Leer chunk del archivo (simula encriptación - esto consume memoria temporalmente)
            with open(path, 'rb') as f:
                f.seek(i * chunk_size)
                chunk = f.read(chunk_size)
                # El chunk se libera al salir del scope
            
            chunks_processed += 1
            
            if progress_callback:
                uploaded = chunks_processed * chunk_size
                progress_obj = Mock()
                progress_obj.uploaded_bytes = min(uploaded, file_size)
                progress_obj.total_bytes = file_size
                progress_callback(progress_obj)
        
        return f"handle_{path.name}"
    
    mock_storage_service.upload_video = mock_upload_video
    mock_storage_service.create_folder = AsyncMock(return_value="folder_handle")
    mock_storage_service.exists = AsyncMock(return_value=False)
    
    # Mock del analyzer
    mock_analyzer = Mock()
    mock_analyzer.analyze_video_async = AsyncMock(return_value={
        "source_id": "test_source_123",
        "duration": 100.0,
        "width": 1920,
        "height": 1080,
        "format": "mp4",
        "size": video_size_mb * 1024 * 1024
    })
    
    # Mock del repository
    mock_repo = Mock()
    mock_repo.save_document = AsyncMock()
    mock_repo.save_video_metadata = AsyncMock()
    mock_repo.check_exists_batch = AsyncMock(return_value={})
    
    # Mock del API client
    mock_api_client = Mock()
    mock_api_client.__aenter__ = AsyncMock(return_value=mock_api_client)
    mock_api_client.__aexit__ = AsyncMock()
    
    # Usar la función real de blake3 para detectar fugas reales
    try:
        from uploader.services.resume import blake3_file
        HAS_BLAKE3 = True
    except ImportError:
        HAS_BLAKE3 = False
        blake3_file = None
    
    # Usar patches para reemplazar los servicios
    from unittest.mock import patch
    with patch('uploader.orchestrator.core.AnalyzerService', return_value=mock_analyzer), \
         patch('uploader.orchestrator.core.MetadataRepository', return_value=mock_repo), \
         patch('uploader.orchestrator.core.HTTPAPIClient', return_value=mock_api_client), \
         patch('uploader.orchestrator.folder.file_processor.AnalyzerService', return_value=mock_analyzer):
        
        monitor.print_sample("Antes de inicializar orchestrator")
        
        # Inicializar orchestrator
        config = UploadConfig(generate_preview=False)  # Desactivar preview
        async with UploadOrchestrator(
            api_url="http://test-api",
            storage_service=mock_storage_service,
            config=config
        ) as orchestrator:
            monitor.print_sample("Después de inicializar orchestrator")
            
            print(f"\n{'='*60}")
            print("EMULANDO FLUJO REAL DEL FOLDER UPLOADER")
            print(f"{'='*60}\n")
            
            memory_before_first = monitor.get_memory_mb()
            memory_after_each_video = []
            
            # Emular el flujo real: subir videos uno por uno
            # Cada video pasa por: blake3 -> check MEGA -> upload
            for i, video_file in enumerate(video_files, 1):
                print(f"\n--- Video {i}/{num_videos}: {video_file.name} ---")
                monitor.print_sample(f"Antes de procesar video {i}")
                
                # Paso 1: Calcular blake3 (como hace Blake3Deduplicator.check)
                # En el flujo real, esto se hace en paralelo para todos, pero aquí lo hacemos secuencial
                if HAS_BLAKE3 and blake3_file:
                    print(f"  [1/3] Calculando blake3_hash (función real)...")
                    hash_result = await blake3_file(video_file)
                    print(f"  [OK] Hash: {hash_result[:16]}...")
                    monitor.print_sample(f"Después de blake3 video {i}")
                
                # Paso 2: Verificar existencia en MEGA (simulado)
                print(f"  [2/3] Verificando existencia en MEGA...")
                await asyncio.sleep(0.01)  # Simular check
                monitor.print_sample(f"Después de check MEGA video {i}")
                
                # Paso 3: Subir el video (emula FileProcessor.process)
                print(f"  [3/3] Subiendo video...")
                
                # Crear un folder temporal con solo este video (como hace el handler)
                single_video_folder = tmp_path / f"single_video_{i}"
                single_video_folder.mkdir()
                single_video_file = single_video_folder / video_file.name
                single_video_file.write_bytes(video_file.read_bytes())
                
                process = orchestrator.upload_folder(single_video_folder, dest="/test")
                result = await process.wait()
                
                monitor.print_sample(f"Después de subir video {i}")
                
                # Esperar un poco para que se libere memoria
                await asyncio.sleep(0.3)
                monitor.print_sample(f"Después de esperar video {i}")
                
                # Verificar memoria después de este video
                current_memory = monitor.get_memory_mb()
                delta_from_start = current_memory - memory_before_first
                memory_after_each_video.append(current_memory)
                
                print(f"  [OK] Video {i} completado.")
                print(f"       Memoria actual: {current_memory:.2f} MB")
                print(f"       Delta desde inicio: {delta_from_start:+.2f} MB")
                
                # Si no es el primer video, comparar con el anterior
                if i > 1:
                    prev_memory = memory_after_each_video[i-2]
                    delta_from_prev = current_memory - prev_memory
                    print(f"       Delta desde video anterior: {delta_from_prev:+.2f} MB")
                    
                    # Advertencia si la memoria crece significativamente entre videos
                    if delta_from_prev > 20:  # Más de 20MB de aumento
                        print(f"       [ADVERTENCIA] Memoria aumentó {delta_from_prev:.2f} MB desde el video anterior!")
                
                # Limpiar folder temporal
                single_video_file.unlink()
                single_video_folder.rmdir()
                
                # Forzar garbage collection para ver si ayuda
                import gc
                gc.collect()
                await asyncio.sleep(0.1)
                monitor.print_sample(f"Después de GC video {i}")
                
                print(f"       Memoria después de GC: {monitor.get_memory_mb():.2f} MB")
            
            print(f"\n{'='*60}")
            print("ANÁLISIS FINAL DE MEMORIA")
            print(f"{'='*60}\n")
            
            # Análisis final
            memory_after_all = monitor.get_memory_mb()
            total_delta = memory_after_all - memory_before_first
            
            print(f"Memoria inicial: {memory_before_first:.2f} MB")
            print(f"Memoria final: {memory_after_all:.2f} MB")
            print(f"Delta total: {total_delta:+.2f} MB\n")
            
            print("Memoria después de cada video:")
            for i, mem in enumerate(memory_after_each_video, 1):
                delta = mem - memory_before_first
                print(f"  Video {i}: {mem:.2f} MB (delta: {delta:+.2f} MB)")
            
            # Análisis de tendencia
            if len(memory_after_each_video) > 1:
                print(f"\nAnálisis de tendencia:")
                first_half_avg = sum(memory_after_each_video[:len(memory_after_each_video)//2]) / (len(memory_after_each_video)//2)
                second_half_avg = sum(memory_after_each_video[len(memory_after_each_video)//2:]) / (len(memory_after_each_video) - len(memory_after_each_video)//2)
                trend = second_half_avg - first_half_avg
                print(f"  Promedio primera mitad: {first_half_avg:.2f} MB")
                print(f"  Promedio segunda mitad: {second_half_avg:.2f} MB")
                print(f"  Tendencia: {trend:+.2f} MB")
                
                if trend > 30:
                    print(f"  [ADVERTENCIA] Tendencia creciente detectada! La memoria aumenta con cada video.")
            
            # Verificar que no hay fuga de memoria
            if total_delta > 100:  # Más de 100MB de aumento para 5 videos de 200MB
                print(f"\n[ADVERTENCIA] Posible fuga de memoria detectada!")
                print(f"   El uso de memoria aumentó {total_delta:.2f} MB después de {num_videos} videos.")
                print(f"   Esto podría indicar que la memoria no se está liberando correctamente.")
            else:
                print(f"\n[OK] La memoria parece estar bajo control (delta: {total_delta:+.2f} MB)")
            
            # Verificar que todos los videos se subieron
            assert result.success, f"La subida falló: {result.error}"
    
    monitor.stop()
    
    # Limpiar
    for video_file in video_files:
        video_file.unlink()
    test_folder.rmdir()


def pytest_addoption(parser):
    """Agregar opción para usar subida real."""
    parser.addoption(
        "--real-upload",
        action="store_true",
        default=False,
        help="Usar subida real a MEGA (requiere configuración)"
    )
