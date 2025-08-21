import pytest
from lazy import LazyCollection


class TestPaginationChunking:
    """Test pagination and chunking functionality"""
    
    def test_basic_pagination_with_skip_take(self):
        """Test basic pagination using skip and take"""
        data = list(range(100))
        page_size = 10
        
        # Page 1 (skip 0, take 10)
        page1 = LazyCollection(data).skip(0).take(page_size).to_list()
        assert page1 == list(range(0, 10)), f"Page 1 failed: {page1}"
        
        # Page 2 (skip 10, take 10)
        page2 = LazyCollection(data).skip(10).take(page_size).to_list()
        assert page2 == list(range(10, 20)), f"Page 2 failed: {page2}"
        
        # Page 3 (skip 20, take 10)
        page3 = LazyCollection(data).skip(20).take(page_size).to_list()
        assert page3 == list(range(20, 30)), f"Page 3 failed: {page3}"
    
    def test_pagination_with_filtering(self):
        """Test pagination on filtered data"""
        data = range(100)
        
        # Get even numbers, then paginate
        even_page1 = (
            LazyCollection(data)
            .filter(lambda x: x % 2 == 0)
            .skip(0)
            .take(5)
            .to_list()
        )
        
        even_page2 = (
            LazyCollection(data)
            .filter(lambda x: x % 2 == 0)
            .skip(5)
            .take(5)
            .to_list()
        )
        
        assert even_page1 == [0, 2, 4, 6, 8], f"Even page 1 failed: {even_page1}"
        assert even_page2 == [10, 12, 14, 16, 18], f"Even page 2 failed: {even_page2}"
    
    def test_batch_processing(self):
        """Test batch processing using take operations"""
        data = range(25)
        batch_size = 5
        batches = []
        
        # Process in batches
        remaining_data = LazyCollection(data)
        for batch_num in range(5):  # 5 batches of 5 items each
            batch = remaining_data.skip(batch_num * batch_size).take(batch_size).to_list()
            if batch:  # If batch is not empty
                batches.append(batch)
        
        expected_batches = [
            [0, 1, 2, 3, 4],
            [5, 6, 7, 8, 9],
            [10, 11, 12, 13, 14],
            [15, 16, 17, 18, 19],
            [20, 21, 22, 23, 24]
        ]
        
        assert batches == expected_batches, f"Batches failed: {batches}"
    
    def test_chunking_with_transformations(self):
        """Test chunking with data transformations"""
        data = range(20)
        
        # Transform then chunk
        chunk1 = (
            LazyCollection(data)
            .map(lambda x: x * x)
            .skip(0)
            .take(5)
            .to_list()
        )
        
        chunk2 = (
            LazyCollection(data)
            .map(lambda x: x * x)
            .skip(5)
            .take(5)
            .to_list()
        )
        
        assert chunk1 == [0, 1, 4, 9, 16], f"Chunk 1 failed: {chunk1}"
        assert chunk2 == [25, 36, 49, 64, 81], f"Chunk 2 failed: {chunk2}"
    
    def test_large_dataset_pagination(self):
        """Test pagination efficiency with large datasets"""
        large_size = 100000
        page_size = 1000
        
        # Get a middle page efficiently
        middle_page = (
            LazyCollection(range(large_size))
            .map(lambda x: x * 2)
            .skip(50000)  # Skip to middle
            .take(page_size)
            .to_list()
        )
        
        assert len(middle_page) == page_size, f"Expected {page_size} items, got {len(middle_page)}"
        assert middle_page[0] == 100000, f"First item should be 100000, got {middle_page[0]}"
        assert middle_page[-1] == 101998, f"Last item should be 101998, got {middle_page[-1]}"
    
    def test_pagination_with_empty_pages(self):
        """Test pagination behavior with empty results"""
        data = range(10)
        
        # Try to get a page beyond the data
        empty_page = (
            LazyCollection(data)
            .skip(20)  # Skip beyond data
            .take(5)
            .to_list()
        )
        
        assert empty_page == [], f"Expected empty page, got {empty_page}"
    
    def test_pagination_with_exact_boundaries(self):
        """Test pagination at exact data boundaries"""
        data = range(20)  # Exactly 20 items
        page_size = 5    # Exactly 4 pages
        
        # Get all 4 pages
        pages = []
        for page_num in range(4):
            page = (
                LazyCollection(data)
                .skip(page_num * page_size)
                .take(page_size)
                .to_list()
            )
            pages.append(page)
        
        expected_pages = [
            [0, 1, 2, 3, 4],
            [5, 6, 7, 8, 9],
            [10, 11, 12, 13, 14],
            [15, 16, 17, 18, 19]
        ]
        
        assert pages == expected_pages, f"Pages failed: {pages}"
        
        # Try one more page (should be empty)
        extra_page = LazyCollection(data).skip(4 * page_size).take(page_size).to_list()
        assert extra_page == [], f"Extra page should be empty, got {extra_page}"
    
    def test_streaming_chunks(self):
        """Test streaming chunk processing without storing all chunks"""
        def process_chunk(items):
            return sum(items)
        
        data = range(100)
        chunk_size = 10
        chunk_sums = []
        
        # Process chunks one at a time
        for i in range(0, 100, chunk_size):
            chunk = (
                LazyCollection(data)
                .skip(i)
                .take(chunk_size)
                .to_list()
            )
            chunk_sums.append(process_chunk(chunk))
        
        # Verify chunk sums
        expected_sums = [45, 145, 245, 345, 445, 545, 645, 745, 845, 945]
        assert chunk_sums == expected_sums, f"Chunk sums failed: {chunk_sums}"
