import { useState, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { 
  Smartphone, 
  Eye, 
  AlertTriangle, 
  ShoppingCart, 
  XOctagon, 
  Search, 
  ScanLine,
  Activity,
  TrendingUp,
  Filter,
  RefreshCw
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
  status: 'found' | 'not_found';
}

export default function StockControlUltra() {
  const { user, isAuthenticated } = useAuth();
  const { toast } = useToast();
  
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [activeFilter, setActiveFilter] = useState<string>('all');
  const [isScanning, setIsScanning] = useState(false);
  const [imeiInput, setImeiInput] = useState("");
  const [scannedItems, setScannedItems] = useState<ScannedItem[]>([]);
  
  const imeiInputRef = useRef<HTMLInputElement>(null);

  // Fetch products
  const { data: products = [], isLoading: productsLoading, refetch } = useQuery({
    queryKey: ['/api/products'],
    queryFn: async () => {
      const response = await apiRequest('GET', `/api/products?clientId=${user?.clientId}`);
      return (response as any) as Product[];
    },
    enabled: !!user?.clientId,
  });

  // Calculate comprehensive stats
  const productArray = Array.isArray(products) ? products : [];
  const stats = {
    total: productArray.length,
    disponible: productArray.filter(p => p.status === 'disponible').length,
    reservado: productArray.filter(p => p.status === 'reservado').length,
    vendido: productArray.filter(p => p.status === 'vendido').length,
    extravios: productArray.filter(p => p.status === 'extravio').length,
    totalValue: productArray.reduce((sum, p) => sum + p.costPrice, 0),
    availableValue: productArray.filter(p => p.status === 'disponible').reduce((sum, p) => sum + p.costPrice, 0),
    topModels: productArray.reduce((acc, p) => {
      acc[p.model] = (acc[p.model] || 0) + 1;
      return acc;
    }, {} as Record<string, number>),
  };

  // Filter products
  const filteredProducts = productArray.filter(product => {
    const matchesFilter = activeFilter === 'all' || product.status === activeFilter;
    const matchesSearch = searchQuery === '' || 
      product.imei.toLowerCase().includes(searchQuery.toLowerCase()) ||
      product.model.toLowerCase().includes(searchQuery.toLowerCase()) ||
      product.color.toLowerCase().includes(searchQuery.toLowerCase());
    
    return matchesFilter && matchesSearch;
  });

  const handleScan = (e: React.FormEvent) => {
    e.preventDefault();
    if (imeiInput.trim()) {
      const existingProduct = productArray.find(p => p.imei === imeiInput.trim());
      const alreadyScanned = scannedItems.find(item => item.imei === imeiInput.trim());
      
      if (!alreadyScanned) {
        const newItem: ScannedItem = {
          imei: imeiInput.trim(),
          product: existingProduct,
          timestamp: new Date(),
          status: existingProduct ? 'found' : 'not_found'
        };
        setScannedItems(prev => [newItem, ...prev]);
        
        toast({
          title: existingProduct ? "IMEI Encontrado" : "IMEI No Encontrado",
          description: existingProduct 
            ? `${existingProduct.model} - ${existingProduct.status}` 
            : `IMEI ${imeiInput.trim()} no está registrado`,
          variant: existingProduct ? "default" : "destructive",
        });
      } else {
        toast({
          title: "IMEI Ya Escaneado",
          description: `Este IMEI ya fue verificado`,
          variant: "destructive",
        });
      }
      setImeiInput("");
    }
  };

  const toggleScanning = () => {
    if (!isScanning) {
      setIsScanning(true);
      setScannedItems([]);
      setTimeout(() => imeiInputRef.current?.focus(), 100);
    } else {
      setIsScanning(false);
      toast({
        title: "Sesión Finalizada",
        description: `Se verificaron ${scannedItems.length} productos`,
      });
    }
  };

  const getStatusIcon = (status: string) => {
    const iconMap: { [key: string]: any } = {
      'disponible': <Eye className="w-5 h-5 text-green-600" />,
      'reservado': <AlertTriangle className="w-5 h-5 text-yellow-600" />,
      'vendido': <ShoppingCart className="w-5 h-5 text-blue-600" />,
      'extravio': <XOctagon className="w-5 h-5 text-red-600" />,
    };
    return iconMap[status] || <Smartphone className="w-5 h-5 text-gray-600" />;
  };

  const getStatusBadge = (status: string) => {
    const badgeMap: { [key: string]: any } = {
      'disponible': <Badge className="bg-green-100 text-green-800 border-green-300">Disponible</Badge>,
      'reservado': <Badge className="bg-yellow-100 text-yellow-800 border-yellow-300">Reservado</Badge>,
      'vendido': <Badge className="bg-blue-100 text-blue-800 border-blue-300">Vendido</Badge>,
      'extravio': <Badge className="bg-red-100 text-red-800 border-red-300">Extravío</Badge>,
    };
    return badgeMap[status] || <Badge variant="outline">{status}</Badge>;
  };

  if (!isAuthenticated) return null;

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex">
        <Sidebar />
        <main className="flex-1 bg-gray-50 dark:bg-gray-900 min-w-0">
          <div className="px-4 sm:px-6 lg:px-8 py-8">
            
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="text-gray-900 dark:text-white">Control de Stock Avanzado</CardTitle>
                    <p className="text-gray-600 dark:text-gray-400 mt-1">
                      Sistema de verificación y gestión de inventario en tiempo real
                    </p>
                  </div>
                  <div className="flex gap-3">
                    <Button onClick={() => refetch()} variant="outline" size="sm">
                      <RefreshCw className="w-4 h-4 mr-2" />
                      Actualizar
                    </Button>
                    <Button 
                      onClick={toggleScanning}
                      className={isScanning ? 'bg-red-600 hover:bg-red-700' : 'bg-blue-600 hover:bg-blue-700'}
                    >
                      <ScanLine className="w-4 h-4 mr-2" />
                      {isScanning ? 'Detener' : 'Iniciar'} Escaneo
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-6">

              {/* Enhanced Statistics Dashboard */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6">
                <Card className="border-l-4 border-l-indigo-500 bg-gradient-to-r from-indigo-50 to-white dark:from-indigo-950/50 dark:to-gray-900 shadow-lg hover:shadow-xl transition-all duration-300">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-semibold text-indigo-600 dark:text-indigo-400 mb-2">INVENTARIO TOTAL</p>
                        <p className="text-3xl font-bold text-indigo-900 dark:text-indigo-100">{stats.total}</p>
                        <p className="text-xs text-indigo-500 mt-1">
                          Valor: ${stats.totalValue.toLocaleString('es-AR')}
                        </p>
                      </div>
                      <div className="p-3 bg-indigo-100 dark:bg-indigo-800/50 rounded-full">
                        <Smartphone className="w-8 h-8 text-indigo-600 dark:text-indigo-300" />
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card className="border-l-4 border-l-green-500 bg-gradient-to-r from-green-50 to-white dark:from-green-950/50 dark:to-gray-900 shadow-lg hover:shadow-xl transition-all duration-300">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-semibold text-green-600 dark:text-green-400 mb-2">DISPONIBLES</p>
                        <p className="text-3xl font-bold text-green-800 dark:text-green-200">{stats.disponible}</p>
                        <p className="text-xs text-green-600 mt-1">
                          {stats.total > 0 ? Math.round((stats.disponible / stats.total) * 100) : 0}% del stock
                        </p>
                      </div>
                      <div className="p-3 bg-green-100 dark:bg-green-800/50 rounded-full">
                        <Eye className="w-8 h-8 text-green-600 dark:text-green-300" />
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card className="border-l-4 border-l-yellow-500 bg-gradient-to-r from-yellow-50 to-white dark:from-yellow-950/50 dark:to-gray-900 shadow-lg hover:shadow-xl transition-all duration-300">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-semibold text-yellow-600 dark:text-yellow-400 mb-2">RESERVADOS</p>
                        <p className="text-3xl font-bold text-yellow-800 dark:text-yellow-200">{stats.reservado}</p>
                        <p className="text-xs text-yellow-600 mt-1">
                          {stats.total > 0 ? Math.round((stats.reservado / stats.total) * 100) : 0}% pendiente
                        </p>
                      </div>
                      <div className="p-3 bg-yellow-100 dark:bg-yellow-800/50 rounded-full">
                        <AlertTriangle className="w-8 h-8 text-yellow-600 dark:text-yellow-300" />
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card className="border-l-4 border-l-blue-500 bg-gradient-to-r from-blue-50 to-white dark:from-blue-950/50 dark:to-gray-900 shadow-lg hover:shadow-xl transition-all duration-300">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-semibold text-blue-600 dark:text-blue-400 mb-2">VENDIDOS</p>
                        <p className="text-3xl font-bold text-blue-800 dark:text-blue-200">{stats.vendido}</p>
                        <p className="text-xs text-blue-600 mt-1">
                          {stats.total > 0 ? Math.round((stats.vendido / stats.total) * 100) : 0}% completado
                        </p>
                      </div>
                      <div className="p-3 bg-blue-100 dark:bg-blue-800/50 rounded-full">
                        <ShoppingCart className="w-8 h-8 text-blue-600 dark:text-blue-300" />
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card className="border-l-4 border-l-red-500 bg-gradient-to-r from-red-50 to-white dark:from-red-950/50 dark:to-gray-900 shadow-lg hover:shadow-xl transition-all duration-300">
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-semibold text-red-600 dark:text-red-400 mb-2">EXTRAVÍOS</p>
                        <p className="text-3xl font-bold text-red-800 dark:text-red-200">{stats.extravios}</p>
                        <p className="text-xs text-red-600 mt-1">
                          {stats.extravios > 0 ? 'Requiere atención' : 'Sin problemas'}
                        </p>
                      </div>
                      <div className="p-3 bg-red-100 dark:bg-red-800/50 rounded-full">
                        <XOctagon className="w-8 h-8 text-red-600 dark:text-red-300" />
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Quick Insights Panel */}
              {productArray.length > 0 && (
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  <Card className="col-span-2 bg-gradient-to-br from-purple-50 to-pink-50 dark:from-purple-950/50 dark:to-pink-950/50 border-purple-200 dark:border-purple-800">
                    <CardHeader>
                      <CardTitle className="flex items-center gap-3 text-purple-800 dark:text-purple-200">
                        <TrendingUp className="w-6 h-6" />
                        Modelos Más Populares
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {Object.entries(stats.topModels)
                          .sort(([,a], [,b]) => b - a)
                          .slice(0, 4)
                          .map(([model, count]) => (
                            <div key={model} className="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-800/70 rounded-lg">
                              <div className="flex-1">
                                <p className="font-medium text-purple-900 dark:text-purple-100">{model}</p>
                                <p className="text-sm text-purple-600 dark:text-purple-400">
                                  {count} unidades • {Math.round((count / productArray.length) * 100)}%
                                </p>
                              </div>
                              <div className="w-12 h-12 bg-purple-100 dark:bg-purple-800/50 rounded-full flex items-center justify-center">
                                <span className="text-lg font-bold text-purple-600 dark:text-purple-300">{count}</span>
                              </div>
                            </div>
                          ))}
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="bg-gradient-to-br from-emerald-50 to-teal-50 dark:from-emerald-950/50 dark:to-teal-950/50 border-emerald-200 dark:border-emerald-800">
                    <CardHeader>
                      <CardTitle className="flex items-center gap-3 text-emerald-800 dark:text-emerald-200">
                        <Activity className="w-6 h-6" />
                        Estado del Inventario
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="space-y-3">
                        <div className="flex justify-between items-center">
                          <span className="text-sm font-medium text-emerald-700 dark:text-emerald-300">Stock Disponible</span>
                          <span className="text-lg font-bold text-emerald-800 dark:text-emerald-200">
                            ${stats.availableValue.toLocaleString('es-AR')}
                          </span>
                        </div>
                        <div className="w-full bg-emerald-100 dark:bg-emerald-800/30 rounded-full h-3">
                          <div 
                            className="bg-gradient-to-r from-emerald-500 to-teal-500 h-3 rounded-full transition-all duration-500"
                            style={{ width: `${stats.total > 0 ? (stats.disponible / stats.total) * 100 : 0}%` }}
                          ></div>
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <div className="text-center">
                            <p className="font-semibold text-emerald-800 dark:text-emerald-200">{stats.disponible}</p>
                            <p className="text-emerald-600 dark:text-emerald-400">Disponibles</p>
                          </div>
                          <div className="text-center">
                            <p className="font-semibold text-emerald-800 dark:text-emerald-200">{stats.total - stats.disponible}</p>
                            <p className="text-emerald-600 dark:text-emerald-400">Procesados</p>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            
            {/* Enhanced Scanner Interface */}
            {isScanning && (
              <Card className="border-2 border-dashed border-cyan-300 dark:border-cyan-600 bg-gradient-to-br from-cyan-50 via-blue-50 to-indigo-50 dark:from-cyan-950/50 dark:via-blue-950/50 dark:to-indigo-950/50 shadow-2xl">
                <CardHeader className="text-center pb-4">
                  <CardTitle className="flex items-center justify-center gap-3 text-2xl text-cyan-800 dark:text-cyan-200">
                    <div className="p-3 bg-cyan-100 dark:bg-cyan-800/50 rounded-full animate-pulse">
                      <Activity className="w-8 h-8 text-cyan-600 dark:text-cyan-300" />
                    </div>
                    Sesión de Escaneo Activa
                  </CardTitle>
                  <p className="text-cyan-600 dark:text-cyan-400 mt-2">
                    Escanea o ingresa códigos IMEI para verificación inmediata
                  </p>
                </CardHeader>
                <CardContent className="space-y-6">
                  <form onSubmit={handleScan} className="flex gap-4 max-w-2xl mx-auto">
                    <div className="flex-1 relative">
                      <Input
                        ref={imeiInputRef}
                        value={imeiInput}
                        onChange={(e) => setImeiInput(e.target.value)}
                        placeholder="Escanear o ingresar IMEI..."
                        className="font-mono text-lg h-14 text-center border-2 border-cyan-200 dark:border-cyan-700 focus:border-cyan-500 dark:focus:border-cyan-400 bg-white/80 dark:bg-gray-800/80 rounded-xl"
                        autoFocus
                      />
                      <ScanLine className="absolute right-4 top-1/2 transform -translate-y-1/2 w-6 h-6 text-cyan-500 dark:text-cyan-400" />
                    </div>
                    <Button type="submit" size="lg" className="px-8 h-14 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-700 hover:to-blue-700 rounded-xl shadow-lg">
                      <ScanLine className="w-5 h-5 mr-2" />
                      Verificar IMEI
                    </Button>
                  </form>

                  {scannedItems.length > 0 && (
                    <Card className="bg-white/70 dark:bg-gray-800/70 backdrop-blur-sm border-cyan-200 dark:border-cyan-700 max-w-4xl mx-auto">
                      <CardHeader className="pb-3">
                        <CardTitle className="flex items-center justify-between">
                          <span className="text-lg text-cyan-800 dark:text-cyan-200">
                            Historial de Verificación
                          </span>
                          <Badge variant="outline" className="text-cyan-700 dark:text-cyan-300 border-cyan-300 dark:border-cyan-600">
                            {scannedItems.length} elementos
                          </Badge>
                        </CardTitle>
                      </CardHeader>
                      <CardContent className="max-h-64 overflow-y-auto">
                        <div className="space-y-3">
                          {scannedItems.slice(0, 8).map((item, index) => (
                            <div key={index} className="flex items-center justify-between p-4 bg-white/80 dark:bg-gray-800/80 rounded-lg border border-cyan-100 dark:border-cyan-800 hover:shadow-md transition-all duration-200">
                              <div className="flex items-center gap-4">
                                <div className="text-xs text-cyan-600 dark:text-cyan-400 font-mono bg-cyan-50 dark:bg-cyan-900/30 px-2 py-1 rounded">
                                  #{String(index + 1).padStart(2, '0')}
                                </div>
                                <code className="text-sm font-medium bg-gray-100 dark:bg-gray-700 px-3 py-1 rounded">
                                  {item.imei}
                                </code>
                                {item.product && (
                                  <div className="text-sm">
                                    <span className="font-medium text-gray-900 dark:text-gray-100">
                                      {item.product.model}
                                    </span>
                                    <span className="text-gray-500 dark:text-gray-400 ml-2">
                                      • {item.product.color}
                                    </span>
                                  </div>
                                )}
                              </div>
                              <div className="flex items-center gap-2">
                                {item.product && getStatusBadge(item.product.status)}
                                {item.status === 'found' ? (
                                  <Badge className="bg-green-100 text-green-800 border-green-300">
                                    <Eye className="w-3 h-3 mr-1" />
                                    Encontrado
                                  </Badge>
                                ) : (
                                  <Badge className="bg-red-100 text-red-800 border-red-300">
                                    <XOctagon className="w-3 h-3 mr-1" />
                                    No Registrado
                                  </Badge>
                                )}
                              </div>
                            </div>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Controls and Filters */}
            <Card>
              <CardContent className="p-6">
                <div className="flex flex-col lg:flex-row gap-4 items-start lg:items-center justify-between">
                  <div className="flex items-center gap-4">
                    <Search className="w-5 h-5 text-gray-500" />
                    <Input
                      placeholder="Buscar por IMEI, modelo o color..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="w-80"
                    />
                  </div>
                  
                  <div className="flex flex-wrap gap-2">
                    {[
                      { key: 'all', label: 'Todos', count: stats.total, color: 'gray' },
                      { key: 'disponible', label: 'Disponible', count: stats.disponible, color: 'green' },
                      { key: 'reservado', label: 'Reservado', count: stats.reservado, color: 'yellow' },
                      { key: 'vendido', label: 'Vendido', count: stats.vendido, color: 'blue' },
                      { key: 'extravio', label: 'Extravíos', count: stats.extravios, color: 'red' },
                    ].map(filter => (
                      <button
                        key={filter.key}
                        onClick={() => setActiveFilter(filter.key)}
                        className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                          activeFilter === filter.key
                            ? `bg-${filter.color}-100 text-${filter.color}-800 border-2 border-${filter.color}-300`
                            : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
                        }`}
                      >
                        {getStatusIcon(filter.key)}
                        {filter.label}
                        <Badge variant="secondary" className="text-xs">
                          {filter.count}
                        </Badge>
                      </button>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Products Table */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-3">
                  <Filter className="w-6 h-6" />
                  Inventario de Productos
                  <Badge variant="outline" className="ml-auto">
                    {filteredProducts.length} productos
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {filteredProducts.length > 0 ? (
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>IMEI</TableHead>
                          <TableHead>Producto</TableHead>
                          <TableHead>Especificaciones</TableHead>
                          <TableHead>Estado</TableHead>
                          <TableHead>Precio</TableHead>
                          <TableHead>Fecha</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredProducts.map((product) => (
                          <TableRow key={product.id} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                            <TableCell>
                              <code className="text-sm bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded">
                                {product.imei}
                              </code>
                            </TableCell>
                            <TableCell>
                              <div className="font-medium">{product.model}</div>
                              <div className="text-sm text-gray-500">{product.color}</div>
                            </TableCell>
                            <TableCell>
                              <div className="text-sm">
                                <div>{product.storage}</div>
                              </div>
                            </TableCell>
                            <TableCell>
                              {getStatusBadge(product.status)}
                            </TableCell>
                            <TableCell>
                              <div className="font-medium">${product.costPrice.toLocaleString()}</div>
                            </TableCell>
                            <TableCell className="text-sm text-gray-500">
                              {new Date(product.createdAt).toLocaleDateString('es-AR')}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                ) : (
                  <div className="text-center py-12">
                    <Smartphone className="w-16 h-16 text-gray-300 mx-auto mb-4" />
                    <h3 className="text-lg font-semibold text-gray-600 dark:text-gray-400 mb-2">
                      No se encontraron productos
                    </h3>
                    <p className="text-gray-500">
                      {searchQuery || activeFilter !== 'all' 
                        ? 'Ajusta los filtros para ver más resultados' 
                        : 'No hay productos registrados en el sistema'
                      }
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            </CardContent>
            </Card>

          </div>
        </main>
      </div>
    </div>
  );
}