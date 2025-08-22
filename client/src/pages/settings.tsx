import { useEffect, useState } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useToast } from "@/hooks/use-toast";
import { Building, Upload, Save, Settings as SettingsIcon } from "lucide-react";
import type { CompanyConfiguration } from "@shared/schema";

export default function Settings() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [formData, setFormData] = useState({
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

  const { data: companyConfig, isLoading } = useQuery({
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

  useEffect(() => {
    if (companyConfig) {
      setFormData({
        companyName: companyConfig.companyName || "",
        cuit: companyConfig.cuit || "",
        address: companyConfig.address || "",
        phone: companyConfig.phone || "",
        email: companyConfig.email || "",
        website: companyConfig.website || "",
        taxCondition: companyConfig.taxCondition || "",
        logoUrl: companyConfig.logoUrl || "",
      });
    }
  }, [companyConfig]);

  const saveConfigMutation = useMutation({
    mutationFn: async (data: typeof formData) => {
      return apiRequest("POST", "/api/company-configuration", {
        ...data,
        clientId: user?.clientId,
      });
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
        description: error.message || "No se pudo guardar la configuración",
        variant: "destructive",
      });
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!formData.companyName || !formData.cuit || !formData.address) {
      toast({
        title: "Error",
        description: "Los campos Nombre de empresa, CUIT y Dirección son obligatorios",
        variant: "destructive",
      });
      return;
    }

    saveConfigMutation.mutate(formData);
  };

  const handleInputChange = (field: keyof typeof formData, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleLogoUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      // In a real app, you would upload the file to a server
      // For now, we'll use a placeholder URL
      const reader = new FileReader();
      reader.onload = (e) => {
        if (e.target?.result) {
          setFormData(prev => ({ ...prev, logoUrl: e.target!.result as string }));
        }
      };
      reader.readAsDataURL(file);
    }
  };

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex">
        <Sidebar />
        
        <main className="flex-1">
          <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            
            <div className="mb-8">
              <div className="flex items-center mb-6">
                <SettingsIcon className="mr-3 h-8 w-8 text-primary" />
                <div>
                  <h1 className="text-3xl font-bold text-gray-900">Configuración</h1>
                  <p className="text-gray-600 mt-1">Gestiona la información de tu empresa</p>
                </div>
              </div>
            </div>

            <Card className="card-hover">
              <CardHeader>
                <CardTitle className="flex items-center text-xl">
                  <Building className="mr-2 h-5 w-5" />
                  Información Empresarial
                </CardTitle>
                <p className="text-sm text-gray-600">
                  Esta información aparecerá en las facturas y documentos oficiales
                </p>
              </CardHeader>
              <CardContent>
                <form onSubmit={handleSubmit} className="space-y-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                      <Label htmlFor="companyName">Nombre de la Empresa *</Label>
                      <Input
                        id="companyName"
                        value={formData.companyName}
                        onChange={(e) => handleInputChange("companyName", e.target.value)}
                        placeholder="Ingresa el nombre de tu empresa"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="cuit">CUIT *</Label>
                      <Input
                        id="cuit"
                        value={formData.cuit}
                        onChange={(e) => handleInputChange("cuit", e.target.value)}
                        placeholder="XX-XXXXXXXX-X"
                        className="mt-1"
                      />
                    </div>

                    <div className="md:col-span-2">
                      <Label htmlFor="address">Dirección *</Label>
                      <Textarea
                        id="address"
                        value={formData.address}
                        onChange={(e) => handleInputChange("address", e.target.value)}
                        placeholder="Dirección completa de la empresa"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="phone">Teléfono</Label>
                      <Input
                        id="phone"
                        value={formData.phone}
                        onChange={(e) => handleInputChange("phone", e.target.value)}
                        placeholder="+54 11 1234-5678"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="email">Email</Label>
                      <Input
                        id="email"
                        type="email"
                        value={formData.email}
                        onChange={(e) => handleInputChange("email", e.target.value)}
                        placeholder="contacto@tuempresa.com"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="website">Sitio Web</Label>
                      <Input
                        id="website"
                        value={formData.website}
                        onChange={(e) => handleInputChange("website", e.target.value)}
                        placeholder="https://www.tuempresa.com"
                        className="mt-1"
                      />
                    </div>

                    <div>
                      <Label htmlFor="taxCondition">Condición Fiscal</Label>
                      <Select value={formData.taxCondition} onValueChange={(value) => handleInputChange("taxCondition", value)}>
                        <SelectTrigger className="mt-1">
                          <SelectValue placeholder="Seleccionar condición fiscal" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="responsable_inscripto">Responsable Inscripto</SelectItem>
                          <SelectItem value="monotributo">Monotributo</SelectItem>
                          <SelectItem value="exento">Exento</SelectItem>
                          <SelectItem value="consumidor_final">Consumidor Final</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="border-t pt-6">
                    <div className="flex items-center justify-between mb-4">
                      <div>
                        <Label>Logo de la Empresa</Label>
                        <p className="text-sm text-gray-600 mt-1">
                          Sube el logo que aparecerá en las facturas (formato PNG, JPG)
                        </p>
                      </div>
                      {formData.logoUrl && (
                        <div className="ml-4">
                          <img
                            src={formData.logoUrl}
                            alt="Logo preview"
                            className="h-16 w-16 object-contain border border-gray-200 rounded"
                          />
                        </div>
                      )}
                    </div>
                    
                    <div className="flex items-center space-x-4">
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => document.getElementById('logoUpload')?.click()}
                      >
                        <Upload className="mr-2 h-4 w-4" />
                        Subir Logo
                      </Button>
                      <input
                        id="logoUpload"
                        type="file"
                        accept="image/*"
                        onChange={handleLogoUpload}
                        className="hidden"
                      />
                      {formData.logoUrl && (
                        <Button
                          type="button"
                          variant="ghost"
                          onClick={() => handleInputChange("logoUrl", "")}
                        >
                          Quitar Logo
                        </Button>
                      )}
                    </div>
                  </div>

                  <div className="flex justify-end pt-6 border-t">
                    <Button 
                      type="submit" 
                      className="gradient-bg text-white"
                      disabled={saveConfigMutation.isPending}
                    >
                      <Save className="mr-2 h-4 w-4" />
                      {saveConfigMutation.isPending ? "Guardando..." : "Guardar Configuración"}
                    </Button>
                  </div>
                </form>
              </CardContent>
            </Card>
            
          </div>
        </main>
      </div>
    </div>
  );
}