//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import { useState } from "react";
import { styled } from "@mui/material/styles";
import CssBaseline from "@mui/material/CssBaseline";
import MuiDrawer from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import MuiAppBar, { AppBarProps as MuiAppBarProps } from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import List from "@mui/material/List";
import IconButton from "@mui/material/IconButton";
import MenuOpenOutlinedIcon from "@mui/icons-material/MenuOpenOutlined";
import MenuOutlinedIcon from "@mui/icons-material/MenuOutlined";
import MenuItems from "../Components/MenuItems";
import { Dialog, Stack, ThemeProvider } from "@mui/material";
import { Outlet, Link } from "react-router-dom";
import Logo from "../assets/Logo";
import Container from "@mui/material/Container";
import ConnectorLink from "../Components/ConnectorLink";
import Breadcrumbs from "../Components/Breadcrumbs";
import { breadcrumbRoutes } from "../App";
import Grid from "@mui/material/Grid";
import useThemeStore from "../store/themeStore";
import { createTheme } from "@mui/material/styles";
import { light, dark, componentSettings } from "./tokens";
import { lightAlpha } from "./colors";
import WalletConnectLink from "../Components/WalletConnectLink";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import useAccountStore from "../store/accountStore";
import { Check } from "@mui/icons-material";

const drawerWidth = 300;

interface AppBarProps extends MuiAppBarProps {
  open?: boolean;
}

const AppBar = styled(MuiAppBar, {
  shouldForwardProp: (prop) => prop !== "open",
})<AppBarProps>(({ theme, open }) => ({
  zIndex: theme.zIndex.drawer + 1,
  transition: theme.transitions.create(["width", "margin"], {
    easing: theme.transitions.easing.easeOut,
    duration: theme.transitions.duration.enteringScreen,
  }),
  ...(open && {
    marginLeft: drawerWidth,
    width: `calc(100% - ${drawerWidth}px)`,
    transition: theme.transitions.create(["width", "margin"], {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
  }),
}));

const Drawer = styled(MuiDrawer, {
  shouldForwardProp: (prop) => prop !== "open",
})(({ theme, open }) => ({
  "& .MuiDrawer-paper": {
    position: "relative",
    whiteSpace: "nowrap",
    borderRight: `1px solid ${lightAlpha[5]}`,
    boxShadow: "10px 14px 28px rgb(35 11 73 / 5%)",
    width: drawerWidth,
    transition: theme.transitions.create("width", {
      easing: theme.transitions.easing.easeOut,
      duration: theme.transitions.duration.enteringScreen,
    }),
    boxSizing: "border-box",
    ...(!open && {
      overflowX: "hidden",
      transition: theme.transitions.create("width", {
        easing: theme.transitions.easing.easeOut,
        duration: theme.transitions.duration.leavingScreen,
      }),
      width: theme.spacing(7),
      [theme.breakpoints.up("sm")]: {
        width: theme.spacing(9),
      },
    }),
  },
}));

export default function Layout() {
  const [open, setOpen] = useState(false);
  const { themeMode } = useThemeStore();
  const { popup, setPopup } = useAccountStore();

  const handleClose = () => {
    setPopup({ visible: false });
  };
  const toggleDrawer = () => {
    setOpen(!open);
  };

  const themeOptions = (mode: string) => {
    return mode === "light" ? light : dark;
  };

  const theme = createTheme({
    ...themeOptions(themeMode),
    ...componentSettings,
  });

  return (
    <ThemeProvider theme={theme}>
      <Dialog open={popup.visible || false} onClose={handleClose}>
        <DialogTitle>
          {popup?.error ? (
            <h2 style={{ color: "red" }}>{popup?.title}</h2>
          ) : (
            <h2>
              <Check style={{ color: "green" }} />
              {popup?.title}
            </h2>
          )}
        </DialogTitle>
        <DialogContent className="dialog-content">{popup?.message}</DialogContent>
      </Dialog>
      <Box sx={{ display: "flex" }}>
        <CssBaseline />
        <AppBar
          position="absolute"
          open={open}
          elevation={0}
          sx={{
            backgroundColor: theme.palette.background.paper,
            boxShadow: "10px 14px 28px rgb(35 11 73 / 5%)",
          }}
        >
          <Toolbar
            sx={{
              pr: "24px",
            }}
          >
            <IconButton
              edge="start"
              color="inherit"
              aria-label="open drawer"
              onClick={toggleDrawer}
              sx={{
                marginRight: "36px",
                color: "#757575",
                ...(open && { display: "none" }),
              }}
            >
              <MenuOutlinedIcon />
            </IconButton>
            <Box
              style={{
                display: "flex",
                justifyContent: "space-between",
                width: "100%",
                alignItems: "center",
              }}
            >
              <Link
                to="/"
                style={{
                  paddingTop: theme.spacing(1),
                }}
              >
                <Logo fill={theme.palette.text.primary} />
              </Link>
              <Stack direction="row" spacing={1}>
                <ConnectorLink />
                <WalletConnectLink />
              </Stack>
            </Box>
          </Toolbar>
        </AppBar>
        <Drawer variant="permanent" open={open}>
          <Toolbar
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "flex-end",
              px: [1],
            }}
          >
            <IconButton onClick={toggleDrawer}>
              <MenuOpenOutlinedIcon />
            </IconButton>
          </Toolbar>
          <List component="nav">
            <MenuItems />
          </List>
        </Drawer>
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            height: "100vh",
            overflow: "auto",
          }}
        >
          <Toolbar />
          <Container
            maxWidth="xl"
            style={{
              paddingTop: theme.spacing(3),
              paddingBottom: theme.spacing(5),
            }}
          >
            <Grid container spacing={3}>
              <Grid item sm={12} md={12} lg={12}>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderBottom: `1px solid ${theme.palette.divider}`,
                  }}
                >
                  <Breadcrumbs items={breadcrumbRoutes} />
                </div>
              </Grid>
              <Outlet />
            </Grid>
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  );
}
