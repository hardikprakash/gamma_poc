import fitz
import pdfplumber
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict
import io
from PIL import Image

@dataclass
class Region:
    """Represents a region on a PDF page"""
    type: str  # 'text', 'table', 'image', 'section_title', 'body_text'
    bbox: Tuple[float, float, float, float]  # (x0, y0, x1, y1)
    order: int
    content: Optional[any] = None
    metadata: Optional[Dict] = None


class PDFIngestor:
    """
    PDF Ingestion and Parsing Module
    Detects regions (text, tables, images) and extracts structured content
    """
    
    def __init__(self, pdf_path: str, document_id: str = None) -> None:
        self.pdf_path = pdf_path
        self.document_id = document_id or pdf_path.split('/')[-1].replace('.pdf', '')
        self.pymupdf_doc = fitz.open(pdf_path)
        self.pdfplumber_doc = pdfplumber.open(pdf_path)
        self.parsed_pages = []
        
        # Configuration
        self.overlap_threshold = 0.5  # 50% overlap for region conflicts
        self.title_font_size_threshold = 14  # Font size above this = likely title
    
    def process_document(self) -> List[Dict]:
        """Process entire document and return structured output"""
        print(f"Processing document: {self.document_id}")
        print(f"Total pages: {len(self.pymupdf_doc)}\n")
        
        for page_num in range(len(self.pymupdf_doc)):
            print(f"Processing page {page_num + 1}/{len(self.pymupdf_doc)}...")
            parsed_page = self.process_page(page_num)
            self.parsed_pages.append(parsed_page)
        
        print(f"\n✓ Document processing complete!")
        return self.parsed_pages
    
    def process_page(self, page_num: int) -> Dict:
        """Process a single page through the full pipeline"""
        pymupdf_page = self.pymupdf_doc[page_num]
        pdfplumber_page = self.pdfplumber_doc.pages[page_num]
        
        # Step 1: Detect all regions
        regions = self._detect_regions(pymupdf_page, pdfplumber_page)
        
        # Step 2: Classify text regions (title vs body)
        regions = self._classify_text_regions(regions, pymupdf_page)
        
        # Step 3: Validate tables
        regions = self._validate_tables(regions, pdfplumber_page)
        
        # Step 4: Extract content from all regions
        regions = self._extract_content(regions, pymupdf_page, pdfplumber_page)
        
        # Step 5: Sort by reading order
        regions = self._sort_by_reading_order(regions)
        
        # Build structured output
        page_data = {
            'document_id': self.document_id,
            'page_num': page_num,
            'regions': [self._region_to_dict(r) for r in regions]
        }
        
        return page_data
    
    def _detect_regions(self, pymupdf_page, pdfplumber_page) -> List[Region]:
        """Detect all regions on the page (tables, text, images)"""
        regions = []
        
        # Priority 1: Detect tables first (they take precedence)
        table_regions = self._detect_tables(pdfplumber_page)
        regions.extend(table_regions)
        
        # Priority 2: Detect text blocks (excluding table areas)
        text_regions = self._detect_text_blocks(pymupdf_page, table_regions)
        regions.extend(text_regions)
        
        # Priority 3: Detect images
        image_regions = self._detect_images(pymupdf_page)
        regions.extend(image_regions)
        
        return regions
    
    def _detect_tables(self, pdfplumber_page) -> List[Region]:
        """Detect table regions using pdfplumber"""
        regions = []
        tables = pdfplumber_page.find_tables()
        
        for i, table in enumerate(tables):
            regions.append(Region(
                type='table',
                bbox=table.bbox,
                order=0,  # Will be set later
                content=table,  # Store table object for extraction
                metadata={'table_index': i}
            ))
        
        return regions
    
    def _detect_text_blocks(self, pymupdf_page, table_regions: List[Region]) -> List[Region]:
        """Detect text blocks, excluding areas covered by tables"""
        regions = []
        blocks = pymupdf_page.get_text("dict")["blocks"]
        
        for block in blocks:
            if block['type'] != 0:  # Skip non-text blocks
                continue
            
            bbox = tuple(block['bbox'])
            
            # Skip if this overlaps significantly with a table
            if self._overlaps_with_regions(bbox, table_regions):
                continue
            
            # Extract text preview and font info
            text_preview = self._get_block_text(block)[:100]
            font_info = self._get_block_font_info(block)
            
            regions.append(Region(
                type='text',  # Will be refined to 'section_title' or 'body_text'
                bbox=bbox,
                order=0,
                content=block,
                metadata={
                    'text_preview': text_preview,
                    'font_size': font_info['size'],
                    'font_name': font_info['name'],
                    'is_bold': font_info['is_bold']
                }
            ))
        
        return regions
    
    def _detect_images(self, pymupdf_page) -> List[Region]:
        """Detect image regions"""
        regions = []
        image_list = pymupdf_page.get_images()
        
        for img_index, img in enumerate(image_list):
            xref = img[0]
            
            # Get image bounding box
            try:
                bbox = pymupdf_page.get_image_bbox(xref)
                if bbox:
                    regions.append(Region(
                        type='image',
                        bbox=tuple(bbox),
                        order=0,
                        content={'xref': xref},
                        metadata={'image_index': img_index}
                    ))
            except:
                # Some images may not have valid bboxes
                continue
        
        return regions
    
    def _classify_text_regions(self, regions: List[Region], pymupdf_page) -> List[Region]:
        """Classify text regions as section_title or body_text"""
        for region in regions:
            if region.type == 'text':
                # Use font size and formatting to determine if it's a title
                font_size = region.metadata.get('font_size', 0)
                is_bold = region.metadata.get('is_bold', False)
                
                if font_size >= self.title_font_size_threshold or is_bold:
                    region.type = 'section_title'
                else:
                    region.type = 'body_text'
        
        return regions
    
    def _validate_tables(self, regions: List[Region], pdfplumber_page) -> List[Region]:
        """Validate that detected tables are real tables (not just formatted text)"""
        validated_regions = []
        
        for region in regions:
            if region.type == 'table':
                # Extract table to check if it's valid
                table = region.content
                bbox = region.bbox
                
                try:
                    cropped = pdfplumber_page.crop(bbox)
                    table_data = cropped.extract_table()
                    
                    # Validate: must have at least 2 rows and 2 columns
                    if table_data and len(table_data) >= 2 and len(table_data[0]) >= 2:
                        validated_regions.append(region)
                    else:
                        # Convert to text region if validation fails
                        region.type = 'body_text'
                        validated_regions.append(region)
                except:
                    # If extraction fails, demote to text
                    region.type = 'body_text'
                    validated_regions.append(region)
            else:
                validated_regions.append(region)
        
        return validated_regions
    
    def _extract_content(self, regions: List[Region], pymupdf_page, pdfplumber_page) -> List[Region]:
        """Extract actual content from all regions"""
        for region in regions:
            try:
                if region.type == 'table':
                    region.content = self._extract_table_content(region, pdfplumber_page)
                
                elif region.type in ['section_title', 'body_text']:
                    region.content = self._extract_text_content(region, pdfplumber_page)
                
                elif region.type == 'image':
                    region.content = self._extract_image_content(region, pymupdf_page)
                
            except Exception as e:
                print(f"Warning: Failed to extract {region.type} content: {e}")
                region.content = None
        
        return regions
    
    def _extract_table_content(self, region: Region, pdfplumber_page) -> Dict:
        """Extract table data"""
        bbox = region.bbox
        cropped = pdfplumber_page.crop(bbox)
        table_data = cropped.extract_table()
        
        return {
            'data': table_data,
            'rows': len(table_data) if table_data else 0,
            'cols': len(table_data[0]) if table_data and table_data[0] else 0
        }
    
    def _extract_text_content(self, region: Region, pdfplumber_page) -> Dict:
        """Extract text content"""
        bbox = region.bbox
        cropped = pdfplumber_page.crop(bbox)
        text = cropped.extract_text()
        
        return {
            'text': text.strip() if text else "",
            'length': len(text.strip()) if text else 0
        }
    
    def _extract_image_content(self, region: Region, pymupdf_page) -> Dict:
        """Extract image content"""
        xref = region.content['xref']
        
        try:
            base_image = pymupdf_page.parent.extract_image(xref)
            image_bytes = base_image["image"]
            
            # Get image dimensions
            image = Image.open(io.BytesIO(image_bytes))
            
            return {
                'image_bytes': image_bytes,
                'format': base_image["ext"],
                'width': image.width,
                'height': image.height,
                'size_kb': len(image_bytes) / 1024
            }
        except Exception as e:
            return {
                'error': str(e)
            }
    
    def _sort_by_reading_order(self, regions: List[Region]) -> List[Region]:
        """Sort regions by reading order (top-to-bottom, left-to-right)"""
        # Sort by y0 (top), then x0 (left)
        sorted_regions = sorted(regions, key=lambda r: (r.bbox[1], r.bbox[0]))
        
        # Assign order numbers
        for i, region in enumerate(sorted_regions):
            region.order = i
        
        return sorted_regions
    
    # Helper methods
    
    def _overlaps_with_regions(self, bbox: Tuple, regions: List[Region]) -> bool:
        """Check if bbox overlaps significantly with any region"""
        for region in regions:
            overlap = self._calculate_overlap(bbox, region.bbox)
            if overlap > self.overlap_threshold:
                return True
        return False
    
    def _calculate_overlap(self, bbox1: Tuple, bbox2: Tuple) -> float:
        """Calculate overlap ratio between two bounding boxes"""
        x0_1, y0_1, x1_1, y1_1 = bbox1
        x0_2, y0_2, x1_2, y1_2 = bbox2
        
        # Calculate intersection
        x0_i = max(x0_1, x0_2)
        y0_i = max(y0_1, y0_2)
        x1_i = min(x1_1, x1_2)
        y1_i = min(y1_1, y1_2)
        
        if x1_i < x0_i or y1_i < y0_i:
            return 0.0  # No overlap
        
        intersection_area = (x1_i - x0_i) * (y1_i - y0_i)
        bbox1_area = (x1_1 - x0_1) * (y1_1 - y0_1)
        
        return intersection_area / bbox1_area if bbox1_area > 0 else 0.0
    
    def _get_block_text(self, block: Dict) -> str:
        """Extract all text from a PyMuPDF block"""
        text = ""
        for line in block.get('lines', []):
            for span in line.get('spans', []):
                text += span.get('text', '')
            text += " "
        return text.strip()
    
    def _get_block_font_info(self, block: Dict) -> Dict:
        """Get font information from first span in block"""
        default_info = {'size': 0, 'name': '', 'is_bold': False}
        
        if not block.get('lines'):
            return default_info
        
        first_line = block['lines'][0]
        if not first_line.get('spans'):
            return default_info
        
        first_span = first_line['spans'][0]
        font_name = first_span.get('font', '').lower()
        
        return {
            'size': first_span.get('size', 0),
            'name': first_span.get('font', ''),
            'is_bold': 'bold' in font_name or 'heavy' in font_name
        }
    
    def _region_to_dict(self, region: Region) -> Dict:
        """Convert Region to dictionary for JSON serialization"""
        data = {
            'type': region.type,
            'bbox': list(region.bbox),
            'order': region.order
        }
        
        # Add content based on type
        if region.type == 'table' and region.content:
            data['table_data'] = region.content.get('data', [])
            data['rows'] = region.content.get('rows', 0)
            data['cols'] = region.content.get('cols', 0)
        
        elif region.type in ['section_title', 'body_text'] and region.content:
            data['text'] = region.content.get('text', '')
        
        elif region.type == 'image' and region.content:
            # Don't include raw bytes in JSON, just metadata
            data['image_format'] = region.content.get('format', '')
            data['width'] = region.content.get('width', 0)
            data['height'] = region.content.get('height', 0)
            data['size_kb'] = round(region.content.get('size_kb', 0), 2)
        
        # Add metadata if present
        if region.metadata:
            data['metadata'] = region.metadata
        
        return data
    
    def get_parsed_pages(self) -> List[Dict]:
        """Return parsed pages"""
        return self.parsed_pages
    
    def close(self):
        """Clean up resources"""
        self.pymupdf_doc.close()
        self.pdfplumber_doc.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
