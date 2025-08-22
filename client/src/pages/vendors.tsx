import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogDescription } from "@/components/ui/dialog";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Separator } from "@/components/ui/separator";
import { Pencil, Trash2, Plus, User, Shield, UserPlus } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { apiRequest } from "@/lib/queryClient";
import { toast } from "@/hooks/use-toast";
import type { Vendor } from "@shared/schema";

const vendorSchema = z.object({
  name: z.string().min(1, "Nombre es requerido"),
  phone: z.string().optional(),
  email: z.union([z.string().email("Email inválido"), z.literal("")]).optional(),
  commissionPercentage: z.string().optional(),
  isActive: z.boolean().default(true),
});

const userSchema = z.object({
  username: z.string().min(1, "Usuario es requerido"),
  email: z.string().email("Email inválido"),
  password: z.string().min(6, "Contraseña mínimo 6 caracteres"),
  permissions: z.array(z.string()).default([]),
});

// Definir las categorías de permisos
const permissionCategories = [
  { id: "dashboard", name: "Dashboard", description: "Acceso al panel principal y estadísticas" },
  { id: "products", name: "Productos", description: "Gestión de inventario y productos" },
  { id: "orders", name: "Pedidos", description: "Creación y gestión de órdenes" },
  { id: "clients", name: "Clientes", description: "Administración de clientes" },
  { id: "cash_register", name: "Caja", description: "Operaciones de caja y conversión" },
  { id: "reports", name: "Reportes", description: "Visualización de reportes" },
  { id: "invoices", name: "Facturas", description: "Generación de facturas" },
  { id: "payments_expenses", name: "Pagos/Gastos", description: "Registro de pagos y gastos" },
  { id: "stock_control", name: "Control Stock", description: "Control físico de inventario" },
];

export default function VendorsPage() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const queryClient = useQueryClient();
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [editingVendor, setEditingVendor] = useState<Vendor | null>(null);
  const [isCreateUserDialogOpen, setIsCreateUserDialogOpen] = useState(false);
  const [editingUserPermissions, setEditingUserPermissions] = useState<any>(null);

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  const { data: vendors, isLoading } = useQuery({
    queryKey: ["/api/vendors", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/vendors?clientId=${user?.clientId}`, {
        headers: {
          'x-user-id': user?.id?.toString() || '',
        },
      });
      if (!response.ok) {
        throw new Error('Failed to fetch vendors');
      }
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: userVendors, isLoading: isLoadingUsers } = useQuery({
    queryKey: ["/api/users/vendors", user?.clientId],
    queryFn: async () => {
      const response = await apiRequest("GET", `/api/users/vendors?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const createVendorMutation = useMutation({
    mutationFn: async (vendorData: any) => {
      const payload = { ...vendorData, clientId: user?.clientId };
      console.log("Creating vendor with data:", payload);
      const response = await apiRequest("POST", "/api/vendors", payload);
      return response.json();
    },
    onSuccess: (data) => {
      console.log("Vendor created successfully:", data);
      queryClient.invalidateQueries({ queryKey: ["/api/vendors", user?.clientId] });
      setIsCreateDialogOpen(false);
      toast({ title: "Vendedor creado exitosamente" });
    },
    onError: (error) => {
      console.error("Error creating vendor:", error);
      toast({ title: "Error al crear vendedor", variant: "destructive" });
    },
  });

  const updateVendorMutation = useMutation({
    mutationFn: async ({ id, ...vendorData }: any) => {
      const response = await apiRequest("PUT", `/api/vendors/${id}`, vendorData);
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/vendors", user?.clientId] });
      setEditingVendor(null);
      toast({ title: "Vendedor actualizado exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al actualizar vendedor", variant: "destructive" });
    },
  });

  const deleteVendorMutation = useMutation({
    mutationFn: async (id: number) => {
      const response = await apiRequest("DELETE", `/api/vendors/${id}`);
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/vendors", user?.clientId] });
      toast({ title: "Vendedor eliminado exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al eliminar vendedor", variant: "destructive" });
    },
  });

  const createUserMutation = useMutation({
    mutationFn: async (userData: any) => {
      const payload = { 
        ...userData, 
        clientId: user?.clientId,
        role: "vendor",
        mustChangePassword: true,
        permissions: JSON.stringify(userData.permissions)
      };
      const response = await apiRequest("POST", "/api/users", payload);
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/users/vendors", user?.clientId] });
      setIsCreateUserDialogOpen(false);
      toast({ title: "Usuario vendedor creado exitosamente" });
    },
    onError: (error: any) => {
      toast({ 
        title: "Error al crear usuario", 
        description: error?.message || "Error desconocido",
        variant: "destructive" 
      });
    },
  });

  const updateUserPermissionsMutation = useMutation({
    mutationFn: async ({ userId, permissions }: any) => {
      const response = await apiRequest("PUT", `/api/users/${userId}/permissions`, {
        permissions: JSON.stringify(permissions)
      });
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/users/vendors", user?.clientId] });
      setEditingUserPermissions(null);
      toast({ title: "Permisos actualizados exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al actualizar permisos", variant: "destructive" });
    },
  });

  const VendorForm = ({ vendor, onClose }: { vendor?: Vendor; onClose: () => void }) => {
    const form = useForm({
      resolver: zodResolver(vendorSchema),
      defaultValues: {
        name: vendor?.name || "",
        phone: vendor?.phone || "",
        email: vendor?.email || "",
        commissionPercentage: vendor?.commissionPercentage || "",
        isActive: vendor?.isActive ?? true,
      },
    });

    const onSubmit = (data: any) => {
      console.log("Form submitted with data:", data);
      console.log("Form errors:", form.formState.errors);
      console.log("Form is valid:", form.formState.isValid);
      
      if (vendor) {
        updateVendorMutation.mutate({ id: vendor.id, ...data });
      } else {
        createVendorMutation.mutate(data);
      }
    };

    return (
      <Form {...form}>
        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
          <FormField
            control={form.control}
            name="name"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Nombre *</FormLabel>
                <FormControl>
                  <Input placeholder="Nombre del vendedor" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="phone"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Teléfono</FormLabel>
                <FormControl>
                  <Input placeholder="Teléfono del vendedor" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="email"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Email</FormLabel>
                <FormControl>
                  <Input placeholder="Email del vendedor" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="commissionPercentage"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Comisión (%)</FormLabel>
                <FormControl>
                  <Input type="number" step="0.01" placeholder="Comisión por venta" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="isActive"
            render={({ field }) => (
              <FormItem className="flex items-center space-x-2 space-y-0">
                <FormControl>
                  <Switch checked={field.value} onCheckedChange={field.onChange} />
                </FormControl>
                <FormLabel>Activo</FormLabel>
              </FormItem>
            )}
          />

          <div className="flex justify-end space-x-2">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancelar
            </Button>
            <Button type="submit" disabled={createVendorMutation.isPending || updateVendorMutation.isPending}>
              {vendor ? "Actualizar" : "Crear"}
            </Button>
          </div>
        </form>
      </Form>
    );
  };

  const UserForm = ({ onClose }: { onClose: () => void }) => {
    const form = useForm({
      resolver: zodResolver(userSchema.extend({
        name: z.string().min(1, "Nombre es requerido"),
        phone: z.string().optional(),
        commissionPercentage: z.string().optional(),
      })),
      defaultValues: {
        name: "",
        username: "",
        email: "",
        password: "",
        phone: "",
        commissionPercentage: "",
        permissions: [],
      },
    });

    const onSubmit = async (data: any) => {
      try {
        // Crear primero el vendedor
        const vendorData = {
          name: data.name,
          phone: data.phone,
          email: data.email,
          commissionPercentage: data.commissionPercentage,
          isActive: true,
          clientId: user?.clientId,
        };
        
        const vendorResponse = await apiRequest("POST", "/api/vendors", vendorData);
        
        // Luego crear el usuario
        const userData = {
          username: data.username,
          email: data.email,
          password: data.password,
          permissions: data.permissions,
          clientId: user?.clientId,
          role: "vendor",
          mustChangePassword: true
        };
        
        await createUserMutation.mutateAsync(userData);
        
        // Invalidar ambas queries
        queryClient.invalidateQueries({ queryKey: ["/api/vendors", user?.clientId] });
        
        toast({ title: "Vendedor y usuario creados exitosamente" });
        onClose();
      } catch (error) {
        toast({ 
          title: "Error al crear vendedor", 
          description: "Error al sincronizar vendedor y usuario",
          variant: "destructive" 
        });
      }
    };

    return (
      <Form {...form}>
        <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={form.control}
                name="name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Nombre *</FormLabel>
                    <FormControl>
                      <Input placeholder="Nombre del vendedor" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="username"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Usuario *</FormLabel>
                    <FormControl>
                      <Input placeholder="Nombre de usuario" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={form.control}
                name="email"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Email *</FormLabel>
                    <FormControl>
                      <Input placeholder="Email del vendedor" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="phone"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Teléfono</FormLabel>
                    <FormControl>
                      <Input placeholder="Teléfono del vendedor" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={form.control}
                name="password"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Contraseña Temporal *</FormLabel>
                    <FormControl>
                      <Input type="password" placeholder="Contraseña temporal" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="commissionPercentage"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Comisión (%)</FormLabel>
                    <FormControl>
                      <Input type="number" step="0.01" placeholder="Comisión por venta" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
          </div>

          <Separator />
          
          <div className="space-y-4">
            <h4 className="font-medium">Permisos de Usuario</h4>
            <p className="text-sm text-muted-foreground">
              Selecciona qué secciones puede acceder este vendedor
            </p>
            
            <FormField
              control={form.control}
              name="permissions"
              render={({ field }) => (
                <FormItem>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {permissionCategories.map((permission) => (
                      <div key={permission.id} className="flex items-start space-x-3 p-3 border rounded-lg">
                        <Checkbox
                          checked={(field.value as string[] || []).includes(permission.id)}
                          onCheckedChange={(checked) => {
                            const currentPermissions = (field.value as string[]) || [];
                            if (checked) {
                              field.onChange([...currentPermissions, permission.id]);
                            } else {
                              field.onChange(currentPermissions.filter((p: string) => p !== permission.id));
                            }
                          }}
                        />
                        <div className="space-y-1 leading-none">
                          <label className="text-sm font-medium cursor-pointer">
                            {permission.name}
                          </label>
                          <p className="text-xs text-muted-foreground">
                            {permission.description}
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                  <FormMessage />
                </FormItem>
              )}
            />
          </div>

          <div className="flex justify-end space-x-2 pt-4">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancelar
            </Button>
            <Button 
              type="submit" 
              disabled={createUserMutation.isPending}
            >
              {createUserMutation.isPending ? "Creando..." : "Crear Vendedor Completo"}
            </Button>
          </div>
        </form>
      </Form>
    );
  };

  const PermissionsModal = ({ userVendor, onClose }: { userVendor: any; onClose: () => void }) => {
    const [selectedPermissions, setSelectedPermissions] = useState<string[]>(
      userVendor?.permissions ? JSON.parse(userVendor.permissions) : []
    );

    const handleSave = () => {
      updateUserPermissionsMutation.mutate({
        userId: userVendor.id,
        permissions: selectedPermissions
      });
    };

    return (
      <div className="space-y-6">
        <div className="space-y-2">
          <h4 className="font-medium">Permisos para {userVendor.username}</h4>
          <p className="text-sm text-muted-foreground">
            Selecciona qué secciones puede acceder este vendedor
          </p>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {permissionCategories.map((permission) => (
            <div key={permission.id} className="flex items-start space-x-3 p-3 border rounded-lg">
              <Checkbox
                checked={selectedPermissions.includes(permission.id)}
                onCheckedChange={(checked) => {
                  if (checked) {
                    setSelectedPermissions([...selectedPermissions, permission.id]);
                  } else {
                    setSelectedPermissions(selectedPermissions.filter(p => p !== permission.id));
                  }
                }}
              />
              <div className="space-y-1 leading-none">
                <label className="text-sm font-medium cursor-pointer">
                  {permission.name}
                </label>
                <p className="text-xs text-muted-foreground">
                  {permission.description}
                </p>
              </div>
            </div>
          ))}
        </div>

        <div className="flex justify-end space-x-2 pt-4">
          <Button type="button" variant="outline" onClick={onClose}>
            Cancelar
          </Button>
          <Button 
            onClick={handleSave}
            disabled={updateUserPermissionsMutation.isPending}
          >
            {updateUserPermissionsMutation.isPending ? "Guardando..." : "Guardar Permisos"}
          </Button>
        </div>
      </div>
    );
  };

  if (!isAuthenticated) {
    return null;
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50">
        <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
        <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
        
        <div className="flex">
          <Sidebar />
          <main className="flex-1 bg-gray-50">
            <div className="flex items-center justify-center h-64">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
            </div>
          </main>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex">
        <Sidebar />
        
        <main className="flex-1 bg-gray-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="space-y-6">
              <div className="flex justify-between items-center">
                <div>
                  <h1 className="text-2xl font-bold">Vendedores</h1>
                  <p className="text-gray-600">Gestiona los vendedores de tu empresa</p>
                </div>
                <div className="flex gap-2">
                  <Dialog open={isCreateUserDialogOpen} onOpenChange={setIsCreateUserDialogOpen}>
                    <DialogTrigger asChild>
                      <Button>
                        <UserPlus className="mr-2 h-4 w-4" />
                        Crear Vendedor
                      </Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
                      <DialogHeader>
                        <DialogTitle>Crear Vendedor Completo</DialogTitle>
                        <DialogDescription>
                          Crea un vendedor con acceso al sistema y permisos
                        </DialogDescription>
                      </DialogHeader>
                      <UserForm onClose={() => setIsCreateUserDialogOpen(false)} />
                    </DialogContent>
                  </Dialog>
                </div>
              </div>

              <Card>
                <CardHeader>
                  <CardTitle>Lista de Vendedores</CardTitle>
                  <CardDescription>
                    Vendedores registrados en el sistema con acceso y permisos
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Nombre</TableHead>
                        <TableHead>Usuario</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Comisión</TableHead>
                        <TableHead>Permisos</TableHead>
                        <TableHead>Estado</TableHead>
                        <TableHead>Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {vendors?.map((vendor: Vendor) => {
                        // Buscar el usuario correspondiente
                        const correspondingUser = userVendors?.find((user: any) => 
                          user.username === vendor.name || user.email === vendor.email
                        );
                        
                        const permissions = correspondingUser?.permissions ? 
                          JSON.parse(correspondingUser.permissions) : [];
                        const permissionNames = permissions.map((p: string) => 
                          permissionCategories.find(cat => cat.id === p)?.name || p
                        );

                        return (
                          <TableRow key={vendor.id}>
                            <TableCell className="font-medium">
                              <div className="flex items-center gap-2">
                                <User className="h-4 w-4" />
                                {vendor.name}
                              </div>
                            </TableCell>
                            <TableCell>
                              {correspondingUser ? (
                                <Badge variant="outline">{correspondingUser.username}</Badge>
                              ) : (
                                <span className="text-muted-foreground text-sm">Sin usuario</span>
                              )}
                            </TableCell>
                            <TableCell>{vendor.email || "-"}</TableCell>
                            <TableCell>
                              {vendor.commissionPercentage ? `${vendor.commissionPercentage}%` : "-"}
                            </TableCell>
                            <TableCell>
                              <div className="flex flex-wrap gap-1">
                                {permissionNames.slice(0, 2).map((permission: string, index: number) => (
                                  <Badge key={index} variant="secondary" className="text-xs">
                                    {permission}
                                  </Badge>
                                ))}
                                {permissionNames.length > 2 && (
                                  <Badge variant="outline" className="text-xs">
                                    +{permissionNames.length - 2}
                                  </Badge>
                                )}
                                {permissionNames.length === 0 && correspondingUser && (
                                  <span className="text-muted-foreground text-xs">Sin permisos</span>
                                )}
                              </div>
                            </TableCell>
                            <TableCell>
                              <div className="flex flex-col gap-1">
                                <Badge variant={vendor.isActive ? "default" : "secondary"}>
                                  {vendor.isActive ? "Activo" : "Inactivo"}
                                </Badge>
                                {correspondingUser?.mustChangePassword && (
                                  <Badge variant="destructive" className="text-xs">
                                    Cambio Pendiente
                                  </Badge>
                                )}
                              </div>
                            </TableCell>
                            <TableCell>
                              <div className="flex space-x-2">
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => setEditingVendor(vendor)}
                                  title="Editar vendedor"
                                >
                                  <Pencil className="h-4 w-4" />
                                </Button>
                                {correspondingUser && (
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => setEditingUserPermissions(correspondingUser)}
                                    title="Editar permisos"
                                  >
                                    <Shield className="h-4 w-4" />
                                  </Button>
                                )}
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={() => deleteVendorMutation.mutate(vendor.id)}
                                  title="Eliminar vendedor"
                                >
                                  <Trash2 className="h-4 w-4 text-red-500" />
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>



              <Dialog open={!!editingVendor} onOpenChange={() => setEditingVendor(null)}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Editar Vendedor</DialogTitle>
                  </DialogHeader>
                  {editingVendor && (
                    <VendorForm vendor={editingVendor} onClose={() => setEditingVendor(null)} />
                  )}
                </DialogContent>
              </Dialog>

              <Dialog open={!!editingUserPermissions} onOpenChange={() => setEditingUserPermissions(null)}>
                <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
                  <DialogHeader>
                    <DialogTitle>Editar Permisos de Usuario</DialogTitle>
                  </DialogHeader>
                  {editingUserPermissions && (
                    <PermissionsModal 
                      userVendor={editingUserPermissions} 
                      onClose={() => setEditingUserPermissions(null)} 
                    />
                  )}
                </DialogContent>
              </Dialog>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}