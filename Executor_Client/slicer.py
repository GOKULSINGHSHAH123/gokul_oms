from typing import Dict, List, Any

class OrderSlicer:
    """Handles slicing large orders into smaller, manageable pieces."""
    
    def __init__(self, max_slice_size: int = 100):
        self.max_slice_size = max_slice_size
    
    def slice_order(self, order: Dict[str, Any], max_slice_size) -> List[Dict[str, Any]]:
        """
        Slice a large order into smaller chunks.
        
        Args:
            order: The original order dictionary
            
        Returns:
            A list of smaller order slices
        """
        total_quantity = order.get('orderQuantity')
        if not total_quantity: return []
        if total_quantity <= max_slice_size:
            return [order]
        
        # Calculate number of slices needed
        num_slices = (total_quantity + max_slice_size - 1) // max_slice_size
        slices = []
        
        for i in range(num_slices):
            # Calculate quantity for this slice
            slice_quantity = min(max_slice_size, total_quantity - i * max_slice_size)
            if slice_quantity <= 0:
                break
                
            # Create a new slice with the same details but adjusted quantity
            order_slice = order.copy()
            order_slice['orderQuantity'] = slice_quantity
            # order_slice['slice_id'] = i + 1
            # order_slice['total_slices'] = num_slices
            # order_slice['parent_order_id'] = order.get('orderUniqueIdentifier')
            
            # Generate a new unique order ID for this slice
            order_slice['orderUniqueIdentifier'] = f"{order.get('orderUniqueIdentifier', 'unknown')}_{i+1}"
            order_slice['orderUniqueIdentifier'] = order_slice['orderUniqueIdentifier'][-20:]
            slices.append(order_slice)
        return slices
