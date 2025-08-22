import { useEffect, useState, useMemo } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/lib/auth";
import { useQuery } from "@tanstack/react-query";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import ProductTable, { SortField, SortDirection } from "@/components/products/product-table";
import ProductFilters, { ProductFilters as Filters } from "@/components/products/product-filters";
import AddProductModal from "@/components/products/add-product-modal";
import BatchStatusModal from "@/components/products/batch-status-modal";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Product } from "@shared/schema";
import { RefreshCw } from "lucide-react";

export default function Products() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [filters, setFilters] = useState<Filters>({});
  const [sortField, setSortField] = useState<SortField>('entryDate');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [batchStatusModalOpen, setBatchStatusModalOpen] = useState(false);
  const [showAdvancedFilters, setShowAdvancedFilters] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  const { data: productsData, isLoading, refetch } = useQuery({
    queryKey: ["/api/products"],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (user?.clientId) params.append("clientId", user.clientId.toString());
      
      const response = await fetch(`/api/products?${params}`);
      if (!response.ok) {
        throw new Error('Failed to fetch products');
      }
      return response.json();
    },
    enabled: !!user?.clientId,
    refetchOnWindowFocus: false,
    staleTime: 0, // Always refetch to ensure fresh data
  });

  // Force data refetch when component mounts to ensure products are visible
  useEffect(() => {
    if (user?.clientId && refetch) {
      refetch();
    }
  }, [user?.clientId, refetch]);

  // Client-side filtering and sorting
  const filteredAndSortedProducts = useMemo(() => {
    if (!productsData) return [];

    let filtered = productsData.filter((product: Product) => {
      // Search filter
      if (search) {
        const searchLower = search.toLowerCase();
        const matchesSearch = 
          product.imei.toLowerCase().includes(searchLower) ||
          product.model.toLowerCase().includes(searchLower) ||
          (product.provider && product.provider.toLowerCase().includes(searchLower));
        if (!matchesSearch) return false;
      }

      // Advanced filters
      if (filters.model && product.model !== filters.model) return false;
      if (filters.storage && product.storage !== filters.storage) return false;
      if (filters.status && product.status !== filters.status) return false;
      if (filters.battery && product.battery !== filters.battery) return false;
      if (filters.quality && product.quality !== filters.quality) return false;
      if (filters.provider && product.provider && 
          !product.provider.toLowerCase().includes(filters.provider.toLowerCase())) return false;
      
      // Price range filter
      if (filters.priceMin || filters.priceMax) {
        const price = parseFloat(product.costPrice);
        if (filters.priceMin && price < parseFloat(filters.priceMin)) return false;
        if (filters.priceMax && price > parseFloat(filters.priceMax)) return false;
      }

      return true;
    });

    // Sort products
    filtered.sort((a: Product, b: Product) => {
      let aValue: any = a[sortField];
      let bValue: any = b[sortField];

      // Handle null/undefined values
      if (aValue === null || aValue === undefined) aValue = '';
      if (bValue === null || bValue === undefined) bValue = '';

      // Special handling for different data types
      if (sortField === 'costPrice') {
        aValue = parseFloat(aValue) || 0;
        bValue = parseFloat(bValue) || 0;
      } else if (sortField === 'entryDate') {
        aValue = new Date(aValue).getTime();
        bValue = new Date(bValue).getTime();
      } else if (typeof aValue === 'string') {
        aValue = aValue.toLowerCase();
        bValue = bValue.toLowerCase();
      }

      if (aValue < bValue) return sortDirection === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortDirection === 'asc' ? 1 : -1;
      return 0;
    });

    return filtered;
  }, [productsData, search, filters, sortField, sortDirection]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  const handleFiltersChange = (newFilters: Filters) => {
    setFilters(newFilters);
  };

  const handleClearFilters = () => {
    setFilters({});
    setSearch('');
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
          <div className="px-4 sm:px-6 lg:px-8 py-8">
            
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-gray-900 dark:text-white">Gestión de Productos</CardTitle>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      onClick={() => setBatchStatusModalOpen(true)}
                    >
                      <RefreshCw className="h-4 w-4 mr-2" />
                      Cambio Masivo
                    </Button>
                    <AddProductModal />
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {/* Search and Filters Section */}
                <div className="space-y-4 mb-6">
                  <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
                    {/* Búsqueda principal */}
                    <div className="relative flex-1 max-w-md">
                      <Input
                        placeholder="Buscar por IMEI, modelo o proveedor..."
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="pl-10"
                      />
                      <svg className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                      </svg>
                    </div>
                    
                    {/* Botones de acción */}
                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        onClick={() => setShowAdvancedFilters(!showAdvancedFilters)}
                        className="flex items-center gap-2"
                      >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.207A1 1 0 013 6.5V4z" />
                        </svg>
                        Filtros Avanzados
                      </Button>
                      
                      {(search || Object.keys(filters).length > 0) && (
                        <Button
                          variant="ghost"
                          onClick={handleClearFilters}
                          className="flex items-center gap-2 text-gray-500"
                        >
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                          </svg>
                          Limpiar
                        </Button>
                      )}
                    </div>
                  </div>

                  {/* Advanced Filters Panel */}
                  {showAdvancedFilters && (
                    <ProductFilters 
                      filters={filters}
                      onFiltersChange={handleFiltersChange}
                      onClearFilters={handleClearFilters}
                      products={productsData || []}
                    />
                  )}

                  {/* Results counter */}
                  <div className="flex justify-between items-center text-sm text-gray-600 dark:text-gray-400">
                    <span>
                      Mostrando {filteredAndSortedProducts.length} de {productsData?.length || 0} productos
                    </span>
                    {(search || Object.keys(filters).length > 0) && (
                      <span className="text-blue-600 dark:text-blue-400">
                        Filtros activos
                      </span>
                    )}
                  </div>
                </div>
                
                <ProductTable 
                  products={filteredAndSortedProducts}
                  isLoading={isLoading}
                  sortField={sortField}
                  sortDirection={sortDirection}
                  onSort={handleSort}
                />
              </CardContent>
            </Card>
            
          </div>
        </main>
      </div>
      
      {/* Batch Status Modal */}
      <BatchStatusModal 
        open={batchStatusModalOpen}
        onOpenChange={setBatchStatusModalOpen}
      />
      <Footer />
    </div>
  );
}
