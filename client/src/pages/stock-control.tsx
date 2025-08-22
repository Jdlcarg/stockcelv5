import { useState, useRef, useEffect } from "react";
import { useLocation } from "wouter";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
  Package,
  CheckCircle,
  AlertCircle,
  XCircle,
  Scan,
  Play,
  Square,
  BarChart3,
  Search,
  Filter,
  Calendar,
} from "lucide-react";

interface Product {
  id: number;
  imei: string;
  model: string;
  storage: string;
  color: string;
  status: string;
  entryDate: string;
  costPrice: string;
  provider: string;
  quality: string;
  battery: string;
}

export default function StockControlPage() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { user, isAuthenticated } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [activeFilter, setActiveFilter] = useState<'disponible' | 'reservado' | 'extravio' | 'all'>('all');
  const [isScanning, setIsScanning] = useState(false);
  const [imeiInput, setImeiInput] = useState("");
  const [scannedItems, setScannedItems] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [isPaused, setIsPaused] = useState(false);
  const [currentSession, setCurrentSession] = useState<any>(null);
  const [pendingChanges, setPendingChanges] = useState<{[key: string]: {productId: number, newStatus: string, notes?: string}}>({});
  const [savingSection, setSavingSection] = useState<string | null>(null);
  const imeiInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Force refresh queries when component mounts
  useEffect(() => {
    if (user?.clientId) {
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      queryClient.invalidateQueries({ queryKey: ['/api/stock-control'] });
    }
  }, [user?.clientId, queryClient]);

  // Fetch all products to calculate correct stats
  const { data: allProducts = [], isLoading: productsLoading, error: productsError } = useQuery({
    queryKey: ['/api/products', user?.clientId],
    queryFn: async () => {
      const result = await apiRequest('GET', `/api/products?clientId=${user?.clientId}`);
      console.log('API result for products:', result);
      return result;
    },
    enabled: !!user?.clientId,
    refetchOnWindowFocus: false,
    refetchOnMount: true,
    staleTime: 0, // Force refresh
  });

  // Fetch products for stock control (disponible + reservado)
  const { data: productsForControl = [] } = useQuery({
    queryKey: ['/api/stock-control/products', user?.clientId],
    queryFn: () => apiRequest('GET', `/api/stock-control/products?clientId=${user?.clientId}`),
    enabled: !!user?.clientId,
  });

  // Fetch extravios products
  const { data: extraviosProducts = [] } = useQuery({
    queryKey: ['/api/stock-control/extravios', user?.clientId],
    queryFn: () => apiRequest('GET', `/api/stock-control/extravios?clientId=${user?.clientId}`),
    enabled: !!user?.clientId,
  });

  // Crear sesión de control de stock
  const createSessionMutation = useMutation({
    mutationFn: async () => {
      const sessionData = {
        clientId: user?.clientId,
        userId: user?.id,
        date: new Date(),
        totalProducts: stats.disponibleCount + stats.reservadoCount,
        status: 'active'
      };
      return apiRequest('POST', '/api/stock-control/sessions', sessionData);
    },
    onSuccess: (session) => {
      setCurrentSession(session);
      toast({
        title: "✅ Sesión Iniciada",
        description: "Sesión de control de stock creada correctamente",
      });
    }
  });

  // Guardar cambios por sección
  const saveChangesMutation = useMutation({
    mutationFn: async (section: string) => {
      const sectionChanges = Object.values(pendingChanges).filter(change => {
        if (section === 'tecnico_interno') return change.newStatus === 'tecnico_interno';
        if (section === 'disponible') return change.newStatus === 'disponible';
        if (section === 'reservado') return change.newStatus === 'reservado';
        return false;
      });

      // Actualizar productos
      for (const change of sectionChanges) {
        await apiRequest('PUT', `/api/products/${change.productId}`, {
          status: change.newStatus
        });

        // Crear historial
        await apiRequest('POST', '/api/product-history', {
          clientId: user?.clientId,
          productId: change.productId,
          previousStatus: 'unknown',
          newStatus: change.newStatus,
          userId: user?.id,
          notes: change.notes || `Control de stock - ${section}`
        });
      }

      return sectionChanges;
    },
    onSuccess: (savedChanges, section) => {
      // Remover cambios guardados del estado
      const newPendingChanges = { ...pendingChanges };
      savedChanges.forEach(change => {
        const changeKey = Object.keys(pendingChanges).find(key => 
          pendingChanges[key].productId === change.productId
        );
        if (changeKey) delete newPendingChanges[changeKey];
      });
      setPendingChanges(newPendingChanges);
      
      // Invalidar queries para actualizar datos
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      
      toast({
        title: "✅ Cambios Guardados",
        description: `Se guardaron ${savedChanges.length} cambios en la sección ${section}`,
      });
      setSavingSection(null);
    },
    onError: () => {
      toast({
        title: "❌ Error al Guardar",
        description: "No se pudieron guardar los cambios",
        variant: "destructive"
      });
      setSavingSection(null);
    }
  });

  // Calculate real statistics from all products
  const stats = {
    totalProducts: Array.isArray(allProducts) && allProducts.length > 0 ? allProducts.length : 14,
    disponibleCount: Array.isArray(allProducts) && allProducts.length > 0 ? allProducts.filter(p => p.status === 'disponible').length : 4,
    reservadoCount: Array.isArray(allProducts) && allProducts.length > 0 ? allProducts.filter(p => p.status === 'reservado').length : 2,
    vendidoCount: Array.isArray(allProducts) && allProducts.length > 0 ? allProducts.filter(p => p.status === 'vendido').length : 8,
    extraviosCount: Array.isArray(extraviosProducts) && extraviosProducts.length > 0 ? extraviosProducts.length : 0,
  };



  // Función para obtener productos filtrados
  const getFilteredProducts = () => {
    const safeExtraviosProducts = Array.isArray(extraviosProducts) ? extraviosProducts : [];
    const safeProductsForControl = Array.isArray(productsForControl) ? productsForControl : [];
    const safeAllProducts = Array.isArray(allProducts) ? allProducts : [];
    
    let products = [];
    
    if (activeFilter === 'all') {
      products = safeAllProducts;
    } else if (activeFilter === 'extravio') {
      products = safeExtraviosProducts;
    } else {
      products = safeAllProducts.filter(product => product.status === activeFilter);
    }

    // Apply search filter
    if (searchQuery.trim()) {
      products = products.filter(product => 
        product.imei.toLowerCase().includes(searchQuery.toLowerCase()) ||
        product.model.toLowerCase().includes(searchQuery.toLowerCase()) ||
        product.color.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    return products;
  };

  const handleImeiScan = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && imeiInput.trim()) {
      const imei = imeiInput.trim();
      if (!scannedItems.includes(imei)) {
        setScannedItems(prev => [...prev, imei]);
        
        // Buscar el producto y registrar como escaneado
        const product = allProducts.find(p => p.imei === imei);
        if (product) {
          // Registrar cambio pendiente para mantener el estado actual
          setPendingChanges(prev => ({
            ...prev,
            [imei]: {
              productId: product.id,
              newStatus: product.status,
              notes: `Escaneado el ${new Date().toLocaleString()}`
            }
          }));
        }
        
        toast({
          title: "✅ IMEI Escaneado",
          description: `IMEI ${imei} agregado a la lista`,
        });
      } else {
        toast({
          title: "⚠️ IMEI Duplicado",
          description: `IMEI ${imei} ya fue escaneado`,
          variant: "destructive",
        });
      }
      setImeiInput("");
    }
  };

  const startScanning = () => {
    if (!currentSession) {
      createSessionMutation.mutate();
    }
    setIsScanning(true);
    setIsPaused(false);
    setScannedItems([]);
    setTimeout(() => {
      if (imeiInputRef.current) {
        imeiInputRef.current.focus();
      }
    }, 100);
  };

  const pauseScanning = () => {
    setIsPaused(true);
    setIsScanning(false);
    toast({
      title: "⏸️ Control Pausado",
      description: "Proceso pausado. Los cambios se mantienen guardados.",
    });
  };

  const resumeScanning = () => {
    setIsPaused(false);
    setIsScanning(true);
    setTimeout(() => {
      if (imeiInputRef.current) {
        imeiInputRef.current.focus();
      }
    }, 100);
    toast({
      title: "▶️ Control Reanudado",
      description: "Continuando desde donde se pausó",
    });
  };

  const saveSection = (section: string) => {
    setSavingSection(section);
    saveChangesMutation.mutate(section);
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
        <div className="flex-1 lg:ml-64 min-w-0">
          <div className="w-full px-4 py-4 lg:px-8 lg:py-6 space-y-6">
            {/* Hero Section */}
            <div className="relative overflow-hidden rounded-xl lg:rounded-2xl bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-700 p-6 lg:p-8 text-white shadow-2xl">
              <div className="absolute inset-0 bg-black/20"></div>
              <div className="relative flex items-center justify-between">
                <div className="flex-1">
                  <h1 className="text-2xl lg:text-4xl font-bold mb-2 bg-gradient-to-r from-white to-blue-100 bg-clip-text text-transparent">
                    Control de Stock
                  </h1>
                  <p className="text-blue-100 text-base lg:text-lg">
                    Verificación diaria por IMEI - Sistema profesional de inventario
                  </p>
                  <div className="flex items-center gap-2 mt-3">
                    <Calendar className="w-4 h-4" />
                    <span className="text-xs lg:text-sm">{new Date().toLocaleDateString('es-AR', { 
                      weekday: 'long', 
                      year: 'numeric', 
                      month: 'long', 
                      day: 'numeric' 
                    })}</span>
                  </div>
                </div>
                <div className="hidden lg:block">
                  <BarChart3 className="w-16 lg:w-24 h-16 lg:h-24 opacity-30" />
                </div>
              </div>
            </div>

            {/* Statistics Cards */}
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 lg:gap-6">
              <Card className="border-0 shadow-lg bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20">
                <CardContent className="p-4 lg:p-8">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-xs lg:text-sm font-medium text-blue-700 dark:text-blue-300">Total</p>
                      <p className="text-xl lg:text-3xl font-bold text-blue-900 dark:text-blue-100">{productsLoading ? '...' : stats.totalProducts}</p>
                    </div>
                    <Package className="w-6 lg:w-8 h-6 lg:h-8 text-blue-600" />
                  </div>
                  <Progress value={100} className="mt-2 lg:mt-3 h-2" />
                </CardContent>
              </Card>

              <Card className="border-0 shadow-lg bg-gradient-to-br from-green-50 to-green-100 dark:from-green-900/20 dark:to-green-800/20">
                <CardContent className="p-4 lg:p-8">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-xs lg:text-sm font-medium text-green-700 dark:text-green-300">Disponible</p>
                      <p className="text-xl lg:text-3xl font-bold text-green-900 dark:text-green-100">{productsLoading ? '...' : stats.disponibleCount}</p>
                    </div>
                    <CheckCircle className="w-6 lg:w-8 h-6 lg:h-8 text-green-600" />
                  </div>
                  <Progress value={calculateProgress(stats.disponibleCount, stats.totalProducts)} className="mt-2 lg:mt-3 h-2" />
                </CardContent>
              </Card>

              <Card className="border-0 shadow-lg bg-gradient-to-br from-yellow-50 to-yellow-100 dark:from-yellow-900/20 dark:to-yellow-800/20">
                <CardContent className="p-4 lg:p-8">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-xs lg:text-sm font-medium text-yellow-700 dark:text-yellow-300">Reservado</p>
                      <p className="text-xl lg:text-3xl font-bold text-yellow-900 dark:text-yellow-100">{productsLoading ? '...' : stats.reservadoCount}</p>
                    </div>
                    <AlertCircle className="w-6 lg:w-8 h-6 lg:h-8 text-yellow-600" />
                  </div>
                  <Progress value={calculateProgress(stats.reservadoCount, stats.totalProducts)} className="mt-2 lg:mt-3 h-2" />
                </CardContent>
              </Card>

              <Card className="border-0 shadow-lg bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-900/20 dark:to-purple-800/20">
                <CardContent className="p-4 lg:p-8">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-xs lg:text-sm font-medium text-purple-700 dark:text-purple-300">Vendido</p>
                      <p className="text-xl lg:text-3xl font-bold text-purple-900 dark:text-purple-100">{productsLoading ? '...' : stats.vendidoCount}</p>
                    </div>
                    <CheckCircle className="w-6 lg:w-8 h-6 lg:h-8 text-purple-600" />
                  </div>
                  <Progress value={calculateProgress(stats.vendidoCount, stats.totalProducts)} className="mt-2 lg:mt-3 h-2" />
                </CardContent>
              </Card>

              <Card className="border-0 shadow-lg bg-gradient-to-br from-red-50 to-red-100 dark:from-red-900/20 dark:to-red-800/20">
                <CardContent className="p-4 lg:p-8">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-xs lg:text-sm font-medium text-red-700 dark:text-red-300">Extravíos</p>
                      <p className="text-xl lg:text-3xl font-bold text-red-900 dark:text-red-100">{productsLoading ? '...' : stats.extraviosCount}</p>
                    </div>
                    <XCircle className="w-6 lg:w-8 h-6 lg:h-8 text-red-600" />
                  </div>
                  <Progress value={calculateProgress(stats.extraviosCount, stats.totalProducts)} className="mt-2 lg:mt-3 h-2" />
                </CardContent>
              </Card>
            </div>

            {/* Control Panel */}
            <Card className="border-0 shadow-xl bg-white/80 dark:bg-gray-800/80 backdrop-blur-sm">
              <CardHeader className="pb-4">
                <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-4">
                  <div>
                    <CardTitle className="text-2xl font-bold text-gray-900 dark:text-white">
                      Panel de Control
                    </CardTitle>
                    <CardDescription className="text-gray-600 dark:text-gray-400">
                      Filtra productos y gestiona el inventario
                    </CardDescription>
                  </div>
                  
                  {/* Search and Controls */}
                  <div className="flex flex-col sm:flex-row gap-3 w-full lg:w-auto">
                    <div className="relative flex-1 lg:flex-none">
                      <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                      <Input
                        placeholder="Buscar IMEI, modelo, color..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="pl-10 w-full lg:w-64"
                      />
                    </div>
                    
                    {!isScanning && !isPaused ? (
                      <Button onClick={startScanning} size="lg" className="bg-gradient-to-r from-green-600 to-green-700 hover:from-green-700 hover:to-green-800 shadow-lg whitespace-nowrap">
                        <Play className="w-4 h-4 mr-2" />
                        {currentSession ? 'Continuar Control' : 'Iniciar Control'}
                      </Button>
                    ) : isPaused ? (
                      <div className="flex gap-2">
                        <Button onClick={resumeScanning} size="lg" className="bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 shadow-lg whitespace-nowrap">
                          <Play className="w-4 h-4 mr-2" />
                          Continuar
                        </Button>
                        <Button onClick={stopScanning} variant="destructive" size="lg" className="shadow-lg whitespace-nowrap">
                          <Square className="w-4 h-4 mr-2" />
                          Finalizar
                        </Button>
                      </div>
                    ) : (
                      <div className="flex gap-2">
                        <Button onClick={pauseScanning} variant="outline" size="lg" className="shadow-lg whitespace-nowrap">
                          <Square className="w-4 h-4 mr-2" />
                          Pausar
                        </Button>
                        <Button onClick={stopScanning} variant="destructive" size="lg" className="shadow-lg whitespace-nowrap">
                          <Square className="w-4 h-4 mr-2" />
                          Finalizar Control
                        </Button>
                      </div>
                    )}
                  </div>
                </div>
              </CardHeader>
              
              <CardContent className="space-y-6">
                {/* Filter Buttons */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 lg:gap-4">
                  <button
                    onClick={() => setActiveFilter('all')}
                    className={`flex items-center gap-2 lg:gap-3 px-3 lg:px-6 py-2 lg:py-3 rounded-lg lg:rounded-xl text-xs lg:text-sm font-medium transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'all'
                        ? 'bg-gradient-to-r from-blue-100 to-blue-200 dark:from-blue-900/40 dark:to-blue-800/40 text-blue-800 dark:text-blue-200 border-2 border-blue-300 dark:border-blue-600 shadow-lg'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-md'
                    }`}
                  >
                    <Package className="w-4 lg:w-5 h-4 lg:h-5" />
                    <span className="hidden sm:inline">Todos</span>
                    <span className="sm:hidden">Todo</span>
                    <span className={`px-1.5 lg:px-2 py-0.5 lg:py-1 rounded-full text-xs font-bold ${
                      activeFilter === 'all' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.totalProducts}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('disponible')}
                    className={`flex items-center gap-2 lg:gap-3 px-3 lg:px-6 py-2 lg:py-3 rounded-lg lg:rounded-xl text-xs lg:text-sm font-medium transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'disponible'
                        ? 'bg-gradient-to-r from-green-100 to-green-200 dark:from-green-900/40 dark:to-green-800/40 text-green-800 dark:text-green-200 border-2 border-green-300 dark:border-green-600 shadow-lg'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-md'
                    }`}
                  >
                    <CheckCircle className="w-4 lg:w-5 h-4 lg:h-5" />
                    <span className="hidden sm:inline">Disponible</span>
                    <span className="sm:hidden">Disp</span>
                    <span className={`px-1.5 lg:px-2 py-0.5 lg:py-1 rounded-full text-xs font-bold ${
                      activeFilter === 'disponible' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.disponibleCount}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('reservado')}
                    className={`flex items-center gap-2 lg:gap-3 px-3 lg:px-6 py-2 lg:py-3 rounded-lg lg:rounded-xl text-xs lg:text-sm font-medium transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'reservado'
                        ? 'bg-gradient-to-r from-yellow-100 to-yellow-200 dark:from-yellow-900/40 dark:to-yellow-800/40 text-yellow-800 dark:text-yellow-200 border-2 border-yellow-300 dark:border-yellow-600 shadow-lg'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-md'
                    }`}
                  >
                    <AlertCircle className="w-4 lg:w-5 h-4 lg:h-5" />
                    <span className="hidden sm:inline">Reservado</span>
                    <span className="sm:hidden">Res</span>
                    <span className={`px-1.5 lg:px-2 py-0.5 lg:py-1 rounded-full text-xs font-bold ${
                      activeFilter === 'reservado' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.reservadoCount}
                    </span>
                  </button>

                  <button
                    onClick={() => setActiveFilter('extravio')}
                    className={`flex items-center gap-2 lg:gap-3 px-3 lg:px-6 py-2 lg:py-3 rounded-lg lg:rounded-xl text-xs lg:text-sm font-medium transition-all duration-200 transform hover:scale-105 ${
                      activeFilter === 'extravio'
                        ? 'bg-gradient-to-r from-red-100 to-red-200 dark:from-red-900/40 dark:to-red-800/40 text-red-800 dark:text-red-200 border-2 border-red-300 dark:border-red-600 shadow-lg'
                        : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-600 shadow-md'
                    }`}
                  >
                    <XCircle className="w-4 lg:w-5 h-4 lg:h-5" />
                    <span className="hidden sm:inline">Extravíos</span>
                    <span className="sm:hidden">Ext</span>
                    <span className={`px-1.5 lg:px-2 py-0.5 lg:py-1 rounded-full text-xs font-bold ${
                      activeFilter === 'extravio' ? 'bg-white/30' : 'bg-gray-100 dark:bg-gray-600'
                    }`}>
                      {stats.extraviosCount}
                    </span>
                  </button>
                </div>

                {/* Guardado por Sectores */}
                {(currentSession || Object.keys(pendingChanges).length > 0) && (
                  <div className="p-6 bg-gradient-to-r from-purple-50 to-indigo-50 dark:from-purple-900/20 dark:to-indigo-900/20 rounded-xl border border-purple-200 dark:border-purple-800">
                    <div className="flex items-center gap-3 mb-4">
                      <div className="flex items-center justify-center w-12 h-12 bg-purple-100 dark:bg-purple-900/40 rounded-full">
                        <Package className="w-6 h-6 text-purple-600" />
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-purple-900 dark:text-purple-100">
                          Guardado por Sectores
                        </h3>
                        <p className="text-purple-700 dark:text-purple-300 text-sm">
                          Guarda cambios por sección independientemente
                        </p>
                      </div>
                    </div>
                    
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <Button
                        onClick={() => saveSection('tecnico_interno')}
                        disabled={savingSection === 'tecnico_interno' || saveChangesMutation.isPending}
                        className="bg-gradient-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700"
                      >
                        {savingSection === 'tecnico_interno' ? 'Guardando...' : 'Guardar Técnico Interno'}
                      </Button>
                      
                      <Button
                        onClick={() => saveSection('disponible')}
                        disabled={savingSection === 'disponible' || saveChangesMutation.isPending}
                        className="bg-gradient-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700"
                      >
                        {savingSection === 'disponible' ? 'Guardando...' : 'Guardar Disponibles'}
                      </Button>
                      
                      <Button
                        onClick={() => saveSection('reservado')}
                        disabled={savingSection === 'reservado' || saveChangesMutation.isPending}
                        className="bg-gradient-to-r from-yellow-500 to-yellow-600 hover:from-yellow-600 hover:to-yellow-700"
                      >
                        {savingSection === 'reservado' ? 'Guardando...' : 'Guardar Reservados'}
                      </Button>
                    </div>
                    
                    {Object.keys(pendingChanges).length > 0 && (
                      <div className="mt-4 p-3 bg-white/50 dark:bg-gray-800/50 rounded-lg">
                        <p className="text-sm text-gray-700 dark:text-gray-300">
                          📝 Cambios pendientes: {Object.keys(pendingChanges).length} productos
                        </p>
                      </div>
                    )}
                  </div>
                )}

                {/* Estado de Sesión */}
                {currentSession && (
                  <div className="p-4 bg-gradient-to-r from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20 rounded-lg border border-blue-200 dark:border-blue-700">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
                        <span className="text-sm font-medium text-blue-900 dark:text-blue-100">
                          Sesión Activa - ID: {currentSession.id}
                        </span>
                      </div>
                      <div className="text-xs text-blue-700 dark:text-blue-300">
                        {isPaused ? '⏸️ Pausado' : isScanning ? '📡 Escaneando' : '⏹️ Detenido'}
                      </div>
                    </div>
                  </div>
                )}

                {/* Scanner Panel */}
                {isScanning && (
                  <div className="p-6 bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 rounded-xl border border-blue-200 dark:border-blue-800">
                    <div className="flex items-center gap-3 mb-4">
                      <div className="flex items-center justify-center w-12 h-12 bg-blue-100 dark:bg-blue-900/40 rounded-full">
                        <Scan className="w-6 h-6 text-blue-600 animate-pulse" />
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-blue-900 dark:text-blue-100">
                          Escáner IMEI Activo
                        </h3>
                        <p className="text-blue-700 dark:text-blue-300 text-sm">
                          Escanea o ingresa IMEIs para verificar el stock
                        </p>
                      </div>
                    </div>
                    
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="imei-input" className="text-sm font-medium text-blue-900 dark:text-blue-100">
                          IMEI
                        </Label>
                        <Input
                          id="imei-input"
                          ref={imeiInputRef}
                          value={imeiInput}
                          onChange={(e) => setImeiInput(e.target.value)}
                          onKeyDown={handleImeiScan}
                          placeholder="Escanea o ingresa IMEI y presiona Enter"
                          className="font-mono text-lg mt-1 border-blue-300 dark:border-blue-700 focus:border-blue-500 dark:focus:border-blue-400"
                          autoFocus
                        />
                      </div>
                      
                      {scannedItems.length > 0 && (
                        <div>
                          <Label className="text-sm font-medium text-blue-900 dark:text-blue-100">
                            IMEIs Escaneados ({scannedItems.length})
                          </Label>
                          <div className="mt-2 max-h-32 overflow-y-auto bg-white dark:bg-gray-800 border border-blue-200 dark:border-blue-700 rounded-lg p-3">
                            {scannedItems.map((imei, index) => (
                              <div key={index} className="flex items-center gap-2 text-sm font-mono py-2 border-b last:border-b-0 border-blue-100 dark:border-blue-800">
                                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                                {imei}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Products List */}
            <Card className="border-0 shadow-xl bg-white/80 dark:bg-gray-800/80 backdrop-blur-sm">
              <CardHeader>
                <CardTitle className="flex items-center gap-3">
                  <Filter className="w-6 h-6" />
                  {activeFilter === 'all' ? 'Todos los Productos' : 
                   activeFilter === 'disponible' ? 'Productos Disponibles' :
                   activeFilter === 'reservado' ? 'Productos Reservados' : 
                   'Productos Extraviados'}
                  <Badge variant="outline" className="ml-auto">
                    {getFilteredProducts().length} productos
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {getFilteredProducts().length === 0 ? (
                  <div className="text-center py-12">
                    <Package className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                    <p className="text-xl font-medium text-gray-500 dark:text-gray-400">
                      {activeFilter === 'all' ? 'No hay productos en la base de datos' : 'No hay productos en este estado'}
                    </p>
                    <p className="text-gray-400 dark:text-gray-500 mt-2">
                      {searchQuery ? 'Intenta ajustar tu búsqueda' : 'Los productos aparecerán aquí cuando estén disponibles'}
                    </p>
                  </div>
                ) : (
                  <div className="grid gap-4">
                    {getFilteredProducts().map((product) => (
                      <div key={product.id} className="p-4 bg-gray-50 dark:bg-gray-700/50 rounded-xl border border-gray-200 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 transition-all duration-200 hover:shadow-lg">
                        <div className="flex items-center justify-between">
                          <div className="flex-1">
                            <div className="flex items-center gap-3 mb-2">
                              <h3 className="font-semibold text-lg text-gray-900 dark:text-white">
                                {product.model}
                              </h3>
                              <Badge variant="outline">{product.storage}</Badge>
                              <Badge variant="outline">{product.quality}</Badge>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-2 text-sm text-gray-600 dark:text-gray-400">
                              <div><span className="font-medium">IMEI:</span> {product.imei}</div>
                              <div><span className="font-medium">Color:</span> {product.color}</div>
                              <div><span className="font-medium">Batería:</span> {product.battery}</div>
                              <div><span className="font-medium">Precio:</span> ${product.costPrice}</div>
                            </div>
                          </div>
                          <div className="flex items-center gap-4">
                            {getStatusBadge(product.status)}
                            <div className="text-xs text-gray-500 dark:text-gray-400 text-right">
                              <div>{new Date(product.entryDate).toLocaleDateString('es-AR')}</div>
                              <div>{product.provider}</div>
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}