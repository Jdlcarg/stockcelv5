import { useState, useEffect } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Settings, DollarSign, Building } from "lucide-react";

export default function Configuration() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  // System configuration state
  const [systemConfig, setSystemConfig] = useState({
    exchangeRate: "1000",
    commission: "5",
    taxRate: "21",
  });

  // Company configuration state
  const [companyConfig, setCompanyConfig] = useState({
    companyName: "",
    cuit: "",
    address: "",
    phone: "",
    email: "",
    website: "",
    taxCondition: "",
    logoUrl: "",
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Fetch system configuration
  const { data: systemConfigData, isLoading: systemConfigLoading } = useQuery({
    queryKey: ["/api/configuration", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/configuration?clientId=${user?.clientId}`);
      if (!response.ok) {
        if (response.status === 404) {
          return {};
        }
        throw new Error("Failed to fetch configuration");
      }
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Fetch company configuration
  const { data: companyConfigData, isLoading: companyConfigLoading } = useQuery({
    queryKey: ["/api/company-configuration", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/company-configuration?clientId=${user?.clientId}`);
      if (!response.ok) {
        if (response.status === 404) {
          return null;
        }
        throw new Error("Failed to fetch company configuration");
      }
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Update state when data is loaded
  useEffect(() => {
    if (systemConfigData) {
      setSystemConfig({
        exchangeRate: systemConfigData.exchangeRate || "1000",
        commission: systemConfigData.commission || "5",
        taxRate: systemConfigData.taxRate || "21",
      });
    }
  }, [systemConfigData]);

  useEffect(() => {
    if (companyConfigData) {
      setCompanyConfig({
        companyName: companyConfigData.companyName || "",
        cuit: companyConfigData.cuit || "",
        address: companyConfigData.address || "",
        phone: companyConfigData.phone || "",
        email: companyConfigData.email || "",
        website: companyConfigData.website || "",
        taxCondition: companyConfigData.taxCondition || "",
        logoUrl: companyConfigData.logoUrl || "",
      });
    }
  }, [companyConfigData]);

  // Save system configuration mutation
  const saveSystemConfigMutation = useMutation({
    mutationFn: async (data: typeof systemConfig) => {
      return apiRequest('POST', '/api/configuration', {
        clientId: user?.clientId,
        configurations: data,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/configuration", user?.clientId] });
      toast({
        title: "Éxito",
        description: "Configuración del sistema guardada correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo guardar la configuración del sistema",
        variant: "destructive",
      });
    },
  });

  // Save company configuration mutation
  const saveCompanyConfigMutation = useMutation({
    mutationFn: async (data: typeof companyConfig) => {
      console.log('Sending company config:', data);
      const response = await apiRequest('POST', '/api/company-configuration', {
        ...data,
        clientId: user?.clientId,
      });
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/company-configuration", user?.clientId] });
      toast({
        title: "Éxito",
        description: "Configuración empresarial guardada correctamente",
      });
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo guardar la configuración empresarial",
        variant: "destructive",
      });
    },
  });

  const handleSystemConfigSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!systemConfig.exchangeRate || parseFloat(systemConfig.exchangeRate) <= 0) {
      toast({
        title: "Error",
        description: "El tipo de cambio debe ser un valor válido mayor a 0",
        variant: "destructive",
      });
      return;
    }

    saveSystemConfigMutation.mutate(systemConfig);
  };

  const handleCompanyConfigSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!companyConfig.companyName || !companyConfig.cuit || !companyConfig.address) {
      toast({
        title: "Error",
        description: "Los campos Nombre de empresa, CUIT y Dirección son obligatorios",
        variant: "destructive",
      });
      return;
    }

    saveCompanyConfigMutation.mutate(companyConfig);
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
        
        <main className="flex-1 bg-gray-50 dark:bg-gray-900">
          <div className="w-full px-8 py-6">
            
            {/* Header */}
            <div className="flex items-center justify-between mb-6 pb-4 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center space-x-4">
                <Settings className="h-8 w-8 text-primary" />
                <div>
                  <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Configuración</h1>
                  <p className="text-gray-600 dark:text-gray-400">Gestiona las configuraciones del sistema y de la empresa</p>
                </div>
              </div>
            </div>

            <Tabs defaultValue="system" className="space-y-6">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="system" className="flex items-center space-x-2">
                  <DollarSign className="h-4 w-4" />
                  <span>Sistema</span>
                </TabsTrigger>
                <TabsTrigger value="company" className="flex items-center space-x-2">
                  <Building className="h-4 w-4" />
                  <span>Empresa</span>
                </TabsTrigger>
              </TabsList>

              {/* System Configuration Tab */}
              <TabsContent value="system">
                <Card>
                  <CardHeader>
                    <CardTitle>Configuración del Sistema</CardTitle>
                    <CardDescription>
                      Configura los parámetros generales del sistema como tipo de cambio y comisiones
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <form onSubmit={handleSystemConfigSubmit} className="space-y-6">
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                        <div className="space-y-2">
                          <Label htmlFor="exchangeRate">Tipo de Cambio USD/ARS</Label>
                          <Input
                            id="exchangeRate"
                            type="number"
                            step="0.01"
                            placeholder="1000.00"
                            value={systemConfig.exchangeRate}
                            onChange={(e) => setSystemConfig(prev => ({
                              ...prev,
                              exchangeRate: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={systemConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="commission">Comisión Vendedor (%)</Label>
                          <Input
                            id="commission"
                            type="number"
                            step="0.1"
                            placeholder="5.0"
                            value={systemConfig.commission}
                            onChange={(e) => setSystemConfig(prev => ({
                              ...prev,
                              commission: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={systemConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="taxRate">Tasa de IVA (%)</Label>
                          <Input
                            id="taxRate"
                            type="number"
                            step="0.1"
                            placeholder="21.0"
                            value={systemConfig.taxRate}
                            onChange={(e) => setSystemConfig(prev => ({
                              ...prev,
                              taxRate: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={systemConfigLoading}
                          />
                        </div>
                      </div>

                      <Separator />

                      <div className="flex justify-end">
                        <Button 
                          type="submit" 
                          disabled={saveSystemConfigMutation.isPending || systemConfigLoading}
                        >
                          {saveSystemConfigMutation.isPending ? "Guardando..." : "Guardar Configuración del Sistema"}
                        </Button>
                      </div>
                    </form>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Company Configuration Tab */}
              <TabsContent value="company">
                <Card>
                  <CardHeader>
                    <CardTitle>Configuración Empresarial</CardTitle>
                    <CardDescription>
                      Configura la información de tu empresa para facturas y documentos
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <form onSubmit={handleCompanyConfigSubmit} className="space-y-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div className="space-y-2">
                          <Label htmlFor="companyName">Nombre de la Empresa *</Label>
                          <Input
                            id="companyName"
                            type="text"
                            placeholder="Mi Empresa S.A."
                            value={companyConfig.companyName}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              companyName: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="cuit">CUIT/CUIL *</Label>
                          <Input
                            id="cuit"
                            type="text"
                            placeholder="20-12345678-9"
                            value={companyConfig.cuit}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              cuit: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2 md:col-span-2">
                          <Label htmlFor="address">Dirección *</Label>
                          <Input
                            id="address"
                            type="text"
                            placeholder="Av. Corrientes 1234, CABA"
                            value={companyConfig.address}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              address: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="phone">Teléfono</Label>
                          <Input
                            id="phone"
                            type="text"
                            placeholder="+54 11 1234-5678"
                            value={companyConfig.phone}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              phone: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="email">Email</Label>
                          <Input
                            id="email"
                            type="email"
                            placeholder="contacto@miempresa.com"
                            value={companyConfig.email}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              email: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="website">Sitio Web</Label>
                          <Input
                            id="website"
                            type="url"
                            placeholder="https://www.miempresa.com"
                            value={companyConfig.website}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              website: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2">
                          <Label htmlFor="taxCondition">Condición Fiscal</Label>
                          <Input
                            id="taxCondition"
                            type="text"
                            placeholder="Responsable Inscripto"
                            value={companyConfig.taxCondition}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              taxCondition: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>

                        <div className="space-y-2 md:col-span-2">
                          <Label htmlFor="logoUrl">URL del Logo</Label>
                          <Input
                            id="logoUrl"
                            type="url"
                            placeholder="https://www.miempresa.com/logo.png"
                            value={companyConfig.logoUrl}
                            onChange={(e) => setCompanyConfig(prev => ({
                              ...prev,
                              logoUrl: e.target.value
                            }))}
                            className="bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600"
                            disabled={companyConfigLoading}
                          />
                        </div>
                      </div>

                      <Separator />

                      <div className="flex justify-end">
                        <Button 
                          type="submit" 
                          disabled={saveCompanyConfigMutation.isPending || companyConfigLoading}
                        >
                          {saveCompanyConfigMutation.isPending ? "Guardando..." : "Guardar Configuración Empresarial"}
                        </Button>
                      </div>
                    </form>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>
        </main>
      </div>
      <Footer />
    </div>
  );
}