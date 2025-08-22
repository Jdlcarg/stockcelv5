import { useState, useRef, useEffect } from "react";
import { useAuth } from "@/hooks/use-auth";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Form, FormControl, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import { Trash2, Save } from "lucide-react";

const baseProductSchema = z.object({
  model: z.string().min(1, "Modelo es requerido"),
  storage: z.string().min(1, "Almacenamiento es requerido"),
  color: z.string().min(1, "Color es requerido"),
  quality: z.string().optional(),
  battery: z.string().optional(),
  costPrice: z.string().min(1, "Precio es requerido"),
  provider: z.string().optional(),
  entryDate: z.date().optional(),
});

type BaseProductFormData = z.infer<typeof baseProductSchema>;

interface ScannedProduct extends BaseProductFormData {
  imei: string;
  id: string; // temporary ID for the scanned list
}

interface ProductFormProps {
  onSuccess?: () => void;
}

export default function ProductForm({ onSuccess }: ProductFormProps) {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const imeiInputRef = useRef<HTMLInputElement>(null);

  const [imeiInput, setImeiInput] = useState("");
  const [scannedProducts, setScannedProducts] = useState<ScannedProduct[]>([]);

  const form = useForm<BaseProductFormData>({
    resolver: zodResolver(baseProductSchema),
    defaultValues: {
      model: "",
      storage: "",
      color: "",
      quality: "",
      battery: "",
      costPrice: "",
      provider: "",
      entryDate: new Date(),
    },
  });

  // Auto-focus IMEI input
  useEffect(() => {
    if (imeiInputRef.current) {
      imeiInputRef.current.focus();
    }
  }, [scannedProducts]);

  const validateImei = async (imei: string): Promise<boolean> => {
    if (imei.length < 10) {
      toast({
        title: "IMEI inválido",
        description: "El IMEI debe tener al menos 10 dígitos",
        variant: "destructive",
      });
      return false;
    }

    // Check if IMEI already exists in scanned products
    if (scannedProducts.some(product => product.imei === imei)) {
      toast({
        title: "IMEI duplicado",
        description: "Este IMEI ya fue escaneado en esta sesión",
        variant: "destructive",
      });
      return false;
    }

    // Check if IMEI exists in database
    try {
      const response = await fetch(`/api/products/check-imei/${imei}?clientId=${user?.clientId}`);
      const data = await response.json();
      
      if (data.exists) {
        toast({
          title: "IMEI ya existe",
          description: "Este IMEI ya está registrado en la base de datos",
          variant: "destructive",
        });
        return false;
      }
    } catch (error) {
      toast({
        title: "Error de validación",
        description: "No se pudo verificar el IMEI",
        variant: "destructive",
      });
      return false;
    }

    return true;
  };

  const handleImeiScan = async (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      
      const formData = form.getValues();
      
      // Validate form data
      const validation = baseProductSchema.safeParse(formData);
      if (!validation.success) {
        toast({
          title: "Campos incompletos",
          description: "Por favor completa todos los campos obligatorios antes de escanear",
          variant: "destructive",
        });
        return;
      }

      const trimmedImei = imeiInput.trim();
      if (!trimmedImei) return;

      const isValid = await validateImei(trimmedImei);
      if (!isValid) return;

      // Create product data and save immediately
      const productData = {
        clientId: user?.clientId,
        imei: trimmedImei,
        model: formData.model,
        storage: formData.storage,
        color: formData.color,
        quality: formData.quality || null,
        battery: formData.battery || null,
        costPrice: formData.costPrice,
        provider: formData.provider || null,
        status: "disponible",
        observations: null,
        entryDate: formData.entryDate || null,
      };

      try {
        // Save product immediately
        await apiRequest("POST", "/api/products", productData);
        
        // Add to scanned products list for display
        const newProduct: ScannedProduct = {
          ...formData,
          imei: trimmedImei,
          id: Date.now().toString() + Math.random(),
        };

        setScannedProducts(prev => [...prev, newProduct]);
        setImeiInput("");
        
        // Invalidate queries to refresh the data
        queryClient.invalidateQueries({ queryKey: ["/api/products"] });
        queryClient.invalidateQueries({ queryKey: ["/api/dashboard/stats"] });
        
        toast({
          title: "Producto guardado",
          description: `IMEI ${trimmedImei} guardado exitosamente`,
        });
      } catch (error: any) {
        toast({
          title: "Error al guardar",
          description: error.message || "No se pudo guardar el producto",
          variant: "destructive",
        });
      }
    }
  };

  const removeScannedProduct = (id: string) => {
    setScannedProducts(prev => prev.filter(product => product.id !== id));
  };

  const clearAllProducts = () => {
    setScannedProducts([]);
    form.reset();
    onSuccess?.();
  };

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Configuración de Productos</CardTitle>
        </CardHeader>
        <CardContent>
          <Form {...form}>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <FormField
                control={form.control}
                name="model"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Modelo</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar modelo" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="iPhone (Original)">iPhone (Original)</SelectItem>
                        <SelectItem value="iPhone 3G">iPhone 3G</SelectItem>
                        <SelectItem value="iPhone 3GS">iPhone 3GS</SelectItem>
                        <SelectItem value="iPhone 4">iPhone 4</SelectItem>
                        <SelectItem value="iPhone 4S">iPhone 4S</SelectItem>
                        <SelectItem value="iPhone 5">iPhone 5</SelectItem>
                        <SelectItem value="iPhone 5C">iPhone 5C</SelectItem>
                        <SelectItem value="iPhone 5S">iPhone 5S</SelectItem>
                        <SelectItem value="iPhone 6">iPhone 6</SelectItem>
                        <SelectItem value="iPhone 6 Plus">iPhone 6 Plus</SelectItem>
                        <SelectItem value="iPhone 6S">iPhone 6S</SelectItem>
                        <SelectItem value="iPhone 6S Plus">iPhone 6S Plus</SelectItem>
                        <SelectItem value="iPhone SE (1ª gen)">iPhone SE (1ª gen)</SelectItem>
                        <SelectItem value="iPhone 7">iPhone 7</SelectItem>
                        <SelectItem value="iPhone 7 Plus">iPhone 7 Plus</SelectItem>
                        <SelectItem value="iPhone 8">iPhone 8</SelectItem>
                        <SelectItem value="iPhone 8 Plus">iPhone 8 Plus</SelectItem>
                        <SelectItem value="iPhone X">iPhone X</SelectItem>
                        <SelectItem value="iPhone XR">iPhone XR</SelectItem>
                        <SelectItem value="iPhone XS">iPhone XS</SelectItem>
                        <SelectItem value="iPhone XS Max">iPhone XS Max</SelectItem>
                        <SelectItem value="iPhone 11">iPhone 11</SelectItem>
                        <SelectItem value="iPhone 11 Pro">iPhone 11 Pro</SelectItem>
                        <SelectItem value="iPhone 11 Pro Max">iPhone 11 Pro Max</SelectItem>
                        <SelectItem value="iPhone SE (2ª gen)">iPhone SE (2ª gen)</SelectItem>
                        <SelectItem value="iPhone 12">iPhone 12</SelectItem>
                        <SelectItem value="iPhone 12 mini">iPhone 12 mini</SelectItem>
                        <SelectItem value="iPhone 12 Pro">iPhone 12 Pro</SelectItem>
                        <SelectItem value="iPhone 12 Pro Max">iPhone 12 Pro Max</SelectItem>
                        <SelectItem value="iPhone 13">iPhone 13</SelectItem>
                        <SelectItem value="iPhone 13 mini">iPhone 13 mini</SelectItem>
                        <SelectItem value="iPhone 13 Pro">iPhone 13 Pro</SelectItem>
                        <SelectItem value="iPhone 13 Pro Max">iPhone 13 Pro Max</SelectItem>
                        <SelectItem value="iPhone SE (3ª gen)">iPhone SE (3ª gen)</SelectItem>
                        <SelectItem value="iPhone 14">iPhone 14</SelectItem>
                        <SelectItem value="iPhone 14 Plus">iPhone 14 Plus</SelectItem>
                        <SelectItem value="iPhone 14 Pro">iPhone 14 Pro</SelectItem>
                        <SelectItem value="iPhone 14 Pro Max">iPhone 14 Pro Max</SelectItem>
                        <SelectItem value="iPhone 15">iPhone 15</SelectItem>
                        <SelectItem value="iPhone 15 Plus">iPhone 15 Plus</SelectItem>
                        <SelectItem value="iPhone 15 Pro">iPhone 15 Pro</SelectItem>
                        <SelectItem value="iPhone 15 Pro Max">iPhone 15 Pro Max</SelectItem>
                        <SelectItem value="iPhone 16">iPhone 16</SelectItem>
                        <SelectItem value="iPhone 16 Plus">iPhone 16 Plus</SelectItem>
                        <SelectItem value="iPhone 16 Pro">iPhone 16 Pro</SelectItem>
                        <SelectItem value="iPhone 16 Pro Max">iPhone 16 Pro Max</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="storage"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Almacenamiento</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar almacenamiento" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="32GB">32GB</SelectItem>
                        <SelectItem value="64GB">64GB</SelectItem>
                        <SelectItem value="128GB">128GB</SelectItem>
                        <SelectItem value="256GB">256GB</SelectItem>
                        <SelectItem value="512GB">512GB</SelectItem>
                        <SelectItem value="1TB">1TB</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="color"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Color</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar color" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="Negro">Negro</SelectItem>
                        <SelectItem value="Gris">Gris</SelectItem>
                        <SelectItem value="Gris espacial">Gris espacial</SelectItem>
                        <SelectItem value="Plata">Plata</SelectItem>
                        <SelectItem value="Dorado">Dorado</SelectItem>
                        <SelectItem value="Oro rosa">Oro rosa</SelectItem>
                        <SelectItem value="Rojo">Rojo</SelectItem>
                        <SelectItem value="Azul">Azul</SelectItem>
                        <SelectItem value="Azul medianoche">Azul medianoche</SelectItem>
                        <SelectItem value="Azul pacífico">Azul pacífico</SelectItem>
                        <SelectItem value="Azul alpino">Azul alpino</SelectItem>
                        <SelectItem value="Verde">Verde</SelectItem>
                        <SelectItem value="Verde medianoche">Verde medianoche</SelectItem>
                        <SelectItem value="Verde montaña">Verde montaña</SelectItem>
                        <SelectItem value="Verde bosque">Verde bosque</SelectItem>
                        <SelectItem value="Verde oliva">Verde oliva</SelectItem>
                        <SelectItem value="Morado">Morado</SelectItem>
                        <SelectItem value="Morado oscuro">Morado oscuro</SelectItem>
                        <SelectItem value="Lavanda">Lavanda</SelectItem>
                        <SelectItem value="Blanco">Blanco</SelectItem>
                        <SelectItem value="Blanco estrella">Blanco estrella</SelectItem>
                        <SelectItem value="Grafito">Grafito</SelectItem>
                        <SelectItem value="Titanio natural">Titanio natural</SelectItem>
                        <SelectItem value="Titanio azul">Titanio azul</SelectItem>
                        <SelectItem value="Titanio negro">Titanio negro</SelectItem>
                        <SelectItem value="Titanio blanco">Titanio blanco</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="quality"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Calidad (Opcional)</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar calidad" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="A">A</SelectItem>
                        <SelectItem value="AB">AB</SelectItem>
                        <SelectItem value="B">B</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="battery"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Batería (Opcional)</FormLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FormControl>
                        <SelectTrigger>
                          <SelectValue placeholder="Seleccionar batería" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        <SelectItem value="-80%">-80%</SelectItem>
                        <SelectItem value="entre 80% y 90%">entre 80% y 90%</SelectItem>
                        <SelectItem value="+90%">+90%</SelectItem>
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="costPrice"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Precio Costo</FormLabel>
                    <FormControl>
                      <Input type="number" step="0.01" placeholder="899.00" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="provider"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Proveedor (Opcional)</FormLabel>
                    <FormControl>
                      <Input placeholder="Nombre del proveedor" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="entryDate"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>Fecha de Ingreso</FormLabel>
                    <FormControl>
                      <Input
                        type="date"
                        value={field.value ? new Date(field.value).toISOString().split('T')[0] : ''}
                        onChange={(e) => field.onChange(e.target.value ? new Date(e.target.value) : null)}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </div>
          </Form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Escanear IMEIs</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div>
              <Label htmlFor="imei-scan">IMEI</Label>
              <Input
                id="imei-scan"
                ref={imeiInputRef}
                placeholder="Escanea o ingresa IMEI aquí..."
                value={imeiInput}
                onChange={(e) => setImeiInput(e.target.value)}
                onKeyDown={handleImeiScan}
                className="text-lg font-mono"
              />
              <p className="text-sm text-gray-600 mt-1">
                Presiona Enter después de escanear cada IMEI
              </p>
            </div>

            {scannedProducts.length > 0 && (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-lg font-semibold">Productos Guardados ({scannedProducts.length})</h3>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={clearAllProducts}
                    disabled={scannedProducts.length === 0}
                  >
                    Limpiar Lista
                  </Button>
                </div>

                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>IMEI</TableHead>
                        <TableHead>Modelo</TableHead>
                        <TableHead>Almacenamiento</TableHead>
                        <TableHead>Color</TableHead>
                        <TableHead>Calidad</TableHead>
                        <TableHead>Batería</TableHead>
                        <TableHead>Precio Costo</TableHead>
                        <TableHead>Proveedor</TableHead>
                        <TableHead>Fecha Ingreso</TableHead>
                        <TableHead className="text-right">Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {scannedProducts.map((product) => (
                        <TableRow key={product.id}>
                          <TableCell className="font-mono">{product.imei}</TableCell>
                          <TableCell>{product.model}</TableCell>
                          <TableCell>{product.storage}</TableCell>
                          <TableCell>{product.color}</TableCell>
                          <TableCell>{product.quality || "-"}</TableCell>
                          <TableCell>{product.battery || "-"}</TableCell>
                          <TableCell>${parseFloat(product.costPrice).toFixed(2)}</TableCell>
                          <TableCell>{product.provider || "-"}</TableCell>
                          <TableCell>{product.entryDate ? product.entryDate.toLocaleDateString() : "-"}</TableCell>
                          <TableCell className="text-right">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => removeScannedProduct(product.id)}
                            >
                              <Trash2 className="h-4 w-4 text-red-600" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
