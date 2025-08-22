import { useState, useRef, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { 
  Package, 
  CheckCircle, 
  AlertCircle, 
  XCircle, 
  Search, 
  Play, 
  Square, 
  Calendar,
  BarChart3,
  Filter,
  Settings,
  RefreshCw,
  ScanLine,
  Eye,
  AlertTriangle
} from "lucide-react";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import { apiRequest } from "@/lib/queryClient";

interface Product {
  id: number;
  clientId: number;
  imei: string;
  model: string;
  storage: string;
  color: string;
  status: 'disponible' | 'reservado' | 'vendido' | 'tecnico_interno' | 'tecnico_externo' | 'a_reparar' | 'extravio';
  costPrice: number;
  salePrice?: number;
  observations?: string;
  createdAt: string;
}

interface ScannedItem {
  imei: string;
  product?: Product;
  timestamp: Date;
}

export default function StockControl() {
  const { user, isAuthenticated } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [activeFilter, setActiveFilter] = useState<string>('all');
  const [isScanning, setIsScanning] = useState(false);
  const [imeiInput, setImeiInput] = useState("");
  const [scannedItems, setScannedItems] = useState<ScannedItem[]>([]);
  
  const imeiInputRef = useRef<HTMLInputElement>(null);

  // Fetch all products
  const { data: allProducts = [], isLoading: productsLoading } = useQuery({
    queryKey: ['/api/products'],
    queryFn: async () => {
      const response = await apiRequest('GET', `/api/products?clientId=${user?.clientId}`);
      return response as Product[];
    },
    enabled: !!user?.clientId,
  });

  // Calculate statistics
  const stats = {
    totalProducts: Array.isArray(allProducts) ? allProducts.length : 0,
    disponibleCount: Array.isArray(allProducts) ? allProducts.filter((p: Product) => p.status === 'disponible').length : 0,
    reservadoCount: Array.isArray(allProducts) ? allProducts.filter((p: Product) => p.status === 'reservado').length : 0,
    vendidoCount: Array.isArray(allProducts) ? allProducts.filter((p: Product) => p.status === 'vendido').length : 0,
    extraviosCount: Array.isArray(allProducts) ? allProducts.filter((p: Product) => p.status === 'extravio').length : 0,
    tecnicoInternoCount: Array.isArray(allProducts) ? allProducts.filter((p: Product) => p.status === 'tecnico_interno').length : 0,
  };

  // Filter products based on active filter and search
  const filteredProducts = Array.isArray(allProducts) ? allProducts.filter((product: Product) => {
    const matchesFilter = activeFilter === 'all' || product.status === activeFilter;
    const matchesSearch = searchQuery === '' || 
      product.imei.toLowerCase().includes(searchQuery.toLowerCase()) ||
      product.model.toLowerCase().includes(searchQuery.toLowerCase()) ||
      product.color.toLowerCase().includes(searchQuery.toLowerCase());
    
    return matchesFilter && matchesSearch;
  }) : [];

  const handleImeiSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (imeiInput.trim()) {
      const existingProduct = Array.isArray(allProducts) ? allProducts.find((p: Product) => p.imei === imeiInput.trim()) : undefined;
      const alreadyScanned = scannedItems.find(item => item.imei === imeiInput.trim());
      
      if (alreadyScanned) {
        toast({
          title: "⚠️ IMEI Duplicado",
          description: `IMEI ${imeiInput.trim()} ya fue escaneado`,
          variant: "destructive",
        });
      } else {
        const newItem: ScannedItem = {
          imei: imeiInput.trim(),
          product: existingProduct,
          timestamp: new Date()
        };
        setScannedItems(prev => [...prev, newItem]);
        
        if (existingProduct && typeof existingProduct === 'object' && 'model' in existingProduct) {
          toast({
            title: "✅ IMEI Encontrado",
            description: `${existingProduct.model} - ${existingProduct.status}`,
          });
        } else {
          toast({
            title: "⚠️ IMEI No Encontrado",
            description: `IMEI ${imeiInput.trim()} no está en la base de datos`,
            variant: "destructive",
          });
        }
      }
      setImeiInput("");
    }
  };

  const startScanning = () => {
    setIsScanning(true);
    setScannedItems([]);
    setTimeout(() => {
      if (imeiInputRef.current) {
        imeiInputRef.current.focus();
      }
    }, 100);
  };

  const stopScanning = () => {
    setIsScanning(false);
    toast({
      title: "🏁 Sesión Finalizada",
      description: `Se escanearon ${scannedItems.length} IMEIs`,
    });
  };

  const getStatusBadge = (status: string) => {
    const variants: { [key: string]: { variant: any; label: string; color: string } } = {
      'disponible': { variant: 'default', label: 'Disponible', color: 'bg-green-500' },
      'reservado': { variant: 'secondary', label: 'Reservado', color: 'bg-yellow-500' },
      'vendido': { variant: 'outline', label: 'Vendido', color: 'bg-blue-500' },
      'tecnico_interno': { variant: 'destructive', label: 'Técnico Interno', color: 'bg-red-500' },
      'tecnico_externo': { variant: 'destructive', label: 'Técnico Externo', color: 'bg-red-500' },
      'a_reparar': { variant: 'destructive', label: 'A Reparar', color: 'bg-orange-500' },
      'extravio': { variant: 'destructive', label: 'Extravío', color: 'bg-red-600' },
    };
    
    const config = variants[status] || { variant: 'outline', label: status, color: 'bg-gray-500' };
    return (
      <div className="flex items-center gap-2">
        <div className={`w-2 h-2 rounded-full ${config.color}`}></div>
        <Badge variant={config.variant}>
          {config.label}
        </Badge>
      </div>
    );
  };

  const calculateProgress = (current: number, total: number) => {
    return total > 0 ? (current / total) * 100 : 0;
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100 dark:from-gray-900 dark:via-gray-800 dark:to-gray-900">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex">
        <Sidebar />
        <main className="flex-1 lg:ml-64 min-w-0">
          <div className="h-full p-4 lg:p-8 space-y-8">
            
            {/* Hero Section - Full Width */}
            <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-700 p-8 text-white shadow-2xl">
              <div className="absolute inset-0 bg-black/20"></div>
              <div className="relative flex items-center justify-between">
                <div className="flex-1">
                  <h1 className="text-4xl font-bold mb-4 bg-gradient-to-r from-white to-blue-100 bg-clip-text text-transparent">
                    Control de Stock
                  </h1>
                  <p className="text-blue-100 text-lg mb-4">
                    Verificación diaria por IMEI - Sistema profesional de inventario
                  </p>
                  <div className="flex items-center gap-3">
                    <Calendar className="w-5 h-5" />
                    <span className="text-sm">{new Date().toLocaleDateString('es-AR', { 
                      weekday: 'long', 
                      year: 'numeric', 
                      month: 'long', 
                      day: 'numeric' 
                    })}</span>
                  </div>
                </div>
                <div className="hidden lg:block">
                  <BarChart3 className="w-32 h-32 opacity-30" />
                </div>
              </div>
            </div>

            {/* Statistics Cards - Full Width Grid */}
            <div className="grid grid-cols-2 md:grid-cols-6 gap-6">
              {/* Total Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/30 dark:to-blue-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-blue-700 dark:text-blue-300 mb-2">Total</p>
                      <p className="text-4xl font-bold text-blue-900 dark:text-blue-100">
                        {productsLoading ? '...' : stats.totalProducts}
                      </p>
                    </div>
                    <Package className="w-12 h-12 text-blue-600" />
                  </div>
                  <Progress value={100} className="h-2" />
                </CardContent>
              </Card>

              {/* Disponible Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/30 dark:to-green-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-green-700 dark:text-green-300 mb-2">Disponible</p>
                      <p className="text-4xl font-bold text-green-900 dark:text-green-100">
                        {productsLoading ? '...' : stats.disponibleCount}
                      </p>
                    </div>
                    <CheckCircle className="w-12 h-12 text-green-600" />
                  </div>
                  <Progress value={calculateProgress(stats.disponibleCount, stats.totalProducts)} className="h-2" />
                </CardContent>
              </Card>

              {/* Reservado Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-yellow-50 to-yellow-100 dark:from-yellow-900/30 dark:to-yellow-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-yellow-700 dark:text-yellow-300 mb-2">Reservado</p>
                      <p className="text-4xl font-bold text-yellow-900 dark:text-yellow-100">
                        {productsLoading ? '...' : stats.reservadoCount}
                      </p>
                    </div>
                    <AlertCircle className="w-12 h-12 text-yellow-600" />
                  </div>
                  <Progress value={calculateProgress(stats.reservadoCount, stats.totalProducts)} className="h-2" />
                </CardContent>
              </Card>

              {/* Técnico Interno Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-orange-50 to-orange-100 dark:from-orange-900/30 dark:to-orange-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-orange-700 dark:text-orange-300 mb-2">Técnico Interno</p>
                      <p className="text-4xl font-bold text-orange-900 dark:text-orange-100">
                        {productsLoading ? '...' : stats.tecnicoInternoCount}
                      </p>
                    </div>
                    <Settings className="w-12 h-12 text-orange-600" />
                  </div>
                  <Progress value={calculateProgress(stats.tecnicoInternoCount, stats.totalProducts)} className="h-2" />
                </CardContent>
              </Card>

              {/* Vendido Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-900/30 dark:to-purple-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-purple-700 dark:text-purple-300 mb-2">Vendido</p>
                      <p className="text-4xl font-bold text-purple-900 dark:text-purple-100">
                        {productsLoading ? '...' : stats.vendidoCount}
                      </p>
                    </div>
                    <CheckCircle className="w-12 h-12 text-purple-600" />
                  </div>
                  <Progress value={calculateProgress(stats.vendidoCount, stats.totalProducts)} className="h-2" />
                </CardContent>
              </Card>

              {/* Extravíos Card */}
              <Card className="border-0 shadow-xl bg-gradient-to-br from-red-50 to-red-100 dark:from-red-900/30 dark:to-red-800/30">
                <CardContent className="p-8">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <p className="text-sm font-medium text-red-700 dark:text-red-300 mb-2">Extravíos</p>
                      <p className="text-4xl font-bold text-red-900 dark:text-red-100">
                        {productsLoading ? '...' : stats.extraviosCount}
                      </p>
                    </div>
                    <XCircle className="w-12 h-12 text-red-600" />
                  </div>
                  <Progress value={calculateProgress(stats.extraviosCount, stats.totalProducts)} className="h-2" />
                </CardContent>
              </Card>
            </div>

            {/* Control Panel - Full Width */}
            <Card className="border-0 shadow-xl bg-white/90 dark:bg-gray-800/90 backdrop-blur-sm">
              <CardHeader className="pb-6">
                <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-6">
                  <div>
                    <CardTitle className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
                      Panel de Control
                    </CardTitle>
                    <CardDescription className="text-gray-600 dark:text-gray-400 text-lg">
                      Filtra productos y gestiona el inventario
                    </CardDescription>
                  </div>
                  
                  {/* Search and Control Actions */}
                  <div className="flex flex-col sm:flex-row gap-4 w-full lg:w-auto">
                    <div className="relative flex-1 lg:flex-none">
                      <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
                      <Input
                        placeholder="Buscar IMEI, modelo, color..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-12 h-12 w-full lg:w-80 text-base"
                      />
                    </div>
                    
                    {!isScanning ? (
                      <Button 
                        onClick={startScanning} 
                        size="lg" 
                        className="bg-gradient-to-r from-green-600 to-green-700 hover:from-green-700 hover:to-green-800 shadow-xl h-12 px-8 text-base font-semibold"
                      >
                        <Play className="w-5 h-5 mr-3" />
                        Iniciar Control
                      </Button>
                    ) : (
                      <Button 
                        onClick={stopScanning} 
                        variant="destructive" 
                        size="lg" 
                        className="shadow-xl h-12 px-8 text-base font-semibold"
                      >
                        <Square className="w-5 h-5 mr-3" />
                        Finalizar Control
                      </Button>
                    )}
                  </div>
                </div>
              </CardHeader>
              
              <CardContent className="space-y-8">
                {/* Filter Buttons - Full Width Grid */}
                <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
                  <button
                    onClick={() => setActiveFilter('all')}
                    className={`flex items-center justify-between p-6 rounded-xl text-base font-semibold transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'all'
                        ? 'bg-gradient-to-r from-blue-100 to-blue-200 dark:from-blue-900/50 dark:to-blue-800/50 text-blue-800 dark:text-blue-200 border-2 border-blue-300 dark:border-blue-600 shadow-xl'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-lg'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <Package className="w-6 h-6" />
                      <span>Todos</span>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                      activeFilter === 'all' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.totalProducts}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('disponible')}
                    className={`flex items-center justify-between p-6 rounded-xl text-base font-semibold transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'disponible'
                        ? 'bg-gradient-to-r from-green-100 to-green-200 dark:from-green-900/50 dark:to-green-800/50 text-green-800 dark:text-green-200 border-2 border-green-300 dark:border-green-600 shadow-xl'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-lg'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <CheckCircle className="w-6 h-6" />
                      <span>Disponible</span>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                      activeFilter === 'disponible' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.disponibleCount}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('reservado')}
                    className={`flex items-center justify-between p-6 rounded-xl text-base font-semibold transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'reservado'
                        ? 'bg-gradient-to-r from-yellow-100 to-yellow-200 dark:from-yellow-900/50 dark:to-yellow-800/50 text-yellow-800 dark:text-yellow-200 border-2 border-yellow-300 dark:border-yellow-600 shadow-xl'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-lg'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <AlertCircle className="w-6 h-6" />
                      <span>Reservado</span>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                      activeFilter === 'reservado' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.reservadoCount}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('extravio')}
                    className={`flex items-center justify-between p-6 rounded-xl text-base font-semibold transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'extravio'
                        ? 'bg-gradient-to-r from-red-100 to-red-200 dark:from-red-900/50 dark:to-red-800/50 text-red-800 dark:text-red-200 border-2 border-red-300 dark:border-red-600 shadow-xl'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-lg'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <XCircle className="w-6 h-6" />
                      <span>Extravíos</span>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                      activeFilter === 'extravio' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.extraviosCount}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('tecnico_interno')}
                    className={`flex items-center justify-between p-6 rounded-xl text-base font-semibold transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'tecnico_interno'
                        ? 'bg-gradient-to-r from-blue-100 to-blue-200 dark:from-blue-900/50 dark:to-blue-800/50 text-blue-800 dark:text-blue-200 border-2 border-blue-300 dark:border-blue-600 shadow-xl'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-lg'
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <Settings className="w-6 h-6" />
                      <span>Técnico Interno</span>
                    </div>
                    <span className={`px-3 py-1 rounded-full text-sm font-bold ${
                      activeFilter === 'tecnico_interno' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.tecnicoInternoCount}
                    </span>
                  </button>
                </div>

                {/* IMEI Scanner - Only when scanning */}
                {isScanning && (
                  <Card className="border-2 border-dashed border-green-300 bg-green-50/50 dark:bg-green-900/20 dark:border-green-600">
                    <CardContent className="p-8">
                      <div className="text-center mb-6">
                        <div className="w-16 h-16 bg-green-100 dark:bg-green-800 rounded-full flex items-center justify-center mx-auto mb-4">
                          <BarChart3 className="w-8 h-8 text-green-600 dark:text-green-400" />
                        </div>
                        <h3 className="text-2xl font-bold text-green-800 dark:text-green-200 mb-2">
                          Modo Escaneo Activo
                        </h3>
                        <p className="text-green-600 dark:text-green-400 text-lg">
                          Escanea o ingresa los IMEIs para verificar el inventario
                        </p>
                      </div>

                      <form onSubmit={handleImeiSubmit} className="flex gap-4 mb-6">
                        <Input
                          ref={imeiInputRef}
                          value={imeiInput}
                          onChange={(e) => setImeiInput(e.target.value)}
                          placeholder="Escanear o ingresar IMEI..."
                          className="flex-1 h-14 text-lg font-mono"
                          autoFocus
                        />
                        <Button type="submit" size="lg" className="h-14 px-8 text-base">
                          Verificar
                        </Button>
                      </form>

                      {scannedItems.length > 0 && (
                        <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-lg">
                          <h4 className="font-semibold text-lg mb-4">
                            Items Escaneados ({scannedItems.length})
                          </h4>
                          <div className="space-y-3 max-h-60 overflow-y-auto">
                            {scannedItems.map((item, index) => (
                              <div key={index} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                                <div className="flex items-center gap-3">
                                  <span className="font-mono text-sm">{item.imei}</span>
                                  {item.product && (
                                    <span className="text-sm text-gray-600 dark:text-gray-400">
                                      {item.product.model}
                                    </span>
                                  )}
                                </div>
                                {item.product ? (
                                  getStatusBadge(item.product.status)
                                ) : (
                                  <Badge variant="destructive">No encontrado</Badge>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}
              </CardContent>
            </Card>

            {/* Products Table - Full Width */}
            <Card className="border-0 shadow-xl">
              <CardHeader className="pb-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <Filter className="w-8 h-8 text-gray-600" />
                    <div>
                      <CardTitle className="text-2xl font-bold">Todos los Productos</CardTitle>
                      <CardDescription className="text-lg">
                        {filteredProducts.length} producto{filteredProducts.length !== 1 ? 's' : ''}
                        {activeFilter !== 'all' && ` (filtrados por ${activeFilter})`}
                      </CardDescription>
                    </div>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {filteredProducts.length > 0 ? (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="text-base font-semibold">IMEI</TableHead>
                          <TableHead className="text-base font-semibold">Modelo</TableHead>
                          <TableHead className="text-base font-semibold">Almacenamiento</TableHead>
                          <TableHead className="text-base font-semibold">Color</TableHead>
                          <TableHead className="text-base font-semibold">Estado</TableHead>
                          <TableHead className="text-base font-semibold">Precio</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredProducts.map((product: Product) => (
                          <TableRow key={product.id} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                            <TableCell className="font-mono text-sm">{product.imei}</TableCell>
                            <TableCell className="font-medium">{product.model}</TableCell>
                            <TableCell>{product.storage}</TableCell>
                            <TableCell>{product.color}</TableCell>
                            <TableCell>{getStatusBadge(product.status)}</TableCell>
                            <TableCell className="font-semibold">
                              ${product.costPrice.toLocaleString()}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                ) : (
                  <div className="text-center py-16">
                    <div className="w-32 h-32 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-6">
                      <Package className="w-16 h-16 text-gray-400" />
                    </div>
                    <h3 className="text-2xl font-semibold text-gray-600 dark:text-gray-400 mb-2">
                      No hay productos en la base de datos
                    </h3>
                    <p className="text-gray-500 dark:text-gray-500">
                      {searchQuery || activeFilter !== 'all' 
                        ? 'No se encontraron productos que coincidan con los filtros aplicados.' 
                        : 'Agrega productos para comenzar a gestionar tu inventario.'
                      }
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Summary Status Cards - Bottom Section */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8" style={{minHeight: '200px'}}>
              {/* Disponible Summary Card */}
              <Card className="border-0 shadow-2xl bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/40 dark:to-green-800/40">
                <CardContent className="p-10">
                  <div className="flex items-center justify-between mb-6">
                    <div>
                      <p className="text-lg font-bold text-green-700 dark:text-green-300 mb-3 uppercase tracking-wide">
                        DISPONIBLE
                      </p>
                      <p className="text-6xl font-bold text-green-900 dark:text-green-100">
                        {productsLoading ? '...' : stats.disponibleCount}
                      </p>
                      <p className="text-green-600 dark:text-green-400 mt-2 text-lg">
                        Para verificación física
                      </p>
                    </div>
                    <div className="bg-green-200 dark:bg-green-800 rounded-full p-6">
                      <Package className="w-16 h-16 text-green-700 dark:text-green-300" />
                    </div>
                  </div>
                  <div className="bg-green-200/50 dark:bg-green-800/30 rounded-xl p-4">
                    <div className="flex items-center gap-3">
                      <Eye className="w-6 h-6 text-green-700 dark:text-green-300" />
                      <span className="text-green-800 dark:text-green-200 font-medium">
                        Listos para verificar
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Reservado Summary Card */}
              <Card className="border-0 shadow-2xl bg-gradient-to-br from-yellow-50 to-yellow-100 dark:from-yellow-900/40 dark:to-yellow-800/40">
                <CardContent className="p-10">
                  <div className="flex items-center justify-between mb-6">
                    <div>
                      <p className="text-lg font-bold text-yellow-700 dark:text-yellow-300 mb-3 uppercase tracking-wide">
                        RESERVADO
                      </p>
                      <p className="text-6xl font-bold text-yellow-900 dark:text-yellow-100">
                        {productsLoading ? '...' : stats.reservadoCount}
                      </p>
                      <p className="text-yellow-600 dark:text-yellow-400 mt-2 text-lg">
                        Para verificación física
                      </p>
                    </div>
                    <div className="bg-yellow-200 dark:bg-yellow-800 rounded-full p-6">
                      <AlertTriangle className="w-16 h-16 text-yellow-700 dark:text-yellow-300" />
                    </div>
                  </div>
                  <div className="bg-yellow-200/50 dark:bg-yellow-800/30 rounded-xl p-4">
                    <div className="flex items-center gap-3">
                      <AlertCircle className="w-6 h-6 text-yellow-700 dark:text-yellow-300" />
                      <span className="text-yellow-800 dark:text-yellow-200 font-medium">
                        Pendientes de entrega
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* TARJETA DE PRUEBA ROJA */}
              <div style={{backgroundColor: 'red', border: '5px solid black', padding: '50px', textAlign: 'center'}}>
                <h1 style={{color: 'white', fontSize: '30px', fontWeight: 'bold'}}>
                  ⚠️ TÉCNICO INTERNO - PRUEBA ⚠️
                </h1>
                <p style={{color: 'yellow', fontSize: '60px', fontWeight: 'bold'}}>
                  {stats.tecnicoInternoCount || 'X'}
                </p>
              </div>

              {/* Técnico Interno Summary Card - DISEÑO IGUAL A DISPONIBLE */}
              <Card className="border-0 shadow-2xl bg-gradient-to-br from-orange-50 to-orange-100 dark:from-orange-900/40 dark:to-orange-800/40">
                <CardContent className="p-10">
                  <div className="flex items-center justify-between mb-6">
                    <div>
                      <p className="text-lg font-bold text-orange-700 dark:text-orange-300 mb-3 uppercase tracking-wide">
                        TÉCNICO INTERNO
                      </p>
                      <p className="text-6xl font-bold text-orange-900 dark:text-orange-100">
                        {productsLoading ? '...' : stats.tecnicoInternoCount}
                      </p>
                      <p className="text-orange-600 dark:text-orange-400 mt-2 text-lg">
                        Para verificación física
                      </p>
                    </div>
                    <div className="bg-orange-200 dark:bg-orange-800 rounded-full p-6">
                      <Settings className="w-16 h-16 text-orange-700 dark:text-orange-300" />
                    </div>
                  </div>
                  <div className="bg-orange-200/50 dark:bg-orange-800/30 rounded-xl p-4">
                    <div className="flex items-center gap-3">
                      <RefreshCw className="w-6 h-6 text-orange-700 dark:text-orange-300" />
                      <span className="text-orange-800 dark:text-orange-200 font-medium">
                        En reparación interna
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>


          </div>
        </main>
      </div>
    </div>
  );
}